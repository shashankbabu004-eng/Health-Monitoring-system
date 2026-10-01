# Smart IoT Health Monitoring & Emergency System (ESP32 FreeRTOS)

An advanced real-time medical monitoring system powered by an **ESP32** dual-core MCU, running **FreeRTOS**. The system concurrently tracks vital signs, environment parameters, controls an motorized bed servo, triggers visual/audible alarms, updates an OLED display, and transmits telemetric data via Adafruit IO MQTT.

---

## Key Features

- **Multi-Tasking Core (FreeRTOS):** Concurrent multi-task execution for sampling sensors, UI management, bed positioning, and cloud telemetry using queues and mutexes.
- **Dedicated Vital Sampling:** Separate tasks monitor Heart Rate, Blood Pressure, Body/Room Temperature, $SpO_2$/Oxygen Levels, and AQI via analog potentiometer simulation.
- **Emergency Automation:** Automatic bed angle adjustment (90° Emergency, 45° Breathing Support) and dynamic sampling rate scaling based on severe patient thresholds.
- **Local OLED Interface:** Shows operational status (`ONLINE`, `DEGRADED`, or `LOGGING OFFLINE`), current patient vitals, active bed angle, sampling rate, and dosage rate.
- **Adafruit IO MQTT Dashboard Integration:** Remote control over dosage levels, target bed angles, and dynamic sampling intervals with telemetric feedback.
- **Network Resilience & Buffering:** Onboard queue buffers telemetry packets during network dropouts and automatically syncs upon reconnection.
- **Hardware Abstraction & Safety:** Timer allocation prevents resource contention between buzzer PWM tones and bed servo motor control.

---

## Hardware & Pin Mapping

| Component / Function | ESP32 Pin | Description |
| :--- | :--- | :--- |
| **Heart Rate Sensor (Pot)** | GPIO 34 | Analog input (Simulated HR: 40–180 BPM) |
| **Blood Pressure Sensor (Pot)** | GPIO 35 | Analog input (Simulated BP: 80–200 mmHg) |
| **Temperature Sensor (Pot)** | GPIO 32 | Analog input (Simulated Body Temp: 25–42°C) |
| **SpO2 Sensor (Pot)** | GPIO 33 | Analog input (Simulated $SpO_2$: 70–100%) |
| **Air Quality Sensor (Pot)** | GPIO 36 (VP) | Analog input (Simulated AQI: 10–400) |
| **I2C SDA (OLED Display)** | GPIO 21 | SSD1306 OLED Data |
| **I2C SCL (OLED Display)** | GPIO 22 | SSD1306 OLED Clock |
| **Bed Actuator Servo** | GPIO 5 | Servo motor angle (0°–90°) |
| **Piezo Buzzer** | GPIO 13 | Active alert tone generator |
| **Status LED Green** | GPIO 12 | Online indicator |
| **Status LED Yellow** | GPIO 14 | Degraded network / reconnecting indicator |
| **Status LED Red** | GPIO 27 | Emergency alarm / offline indicator |
| **WiFi Toggle Button** | GPIO 15 | Push button (Internal Pull-Up) for network testing |

---
#include <Wire.h>
#include <Adafruit_GFX.h>
#include <Adafruit_SSD1306.h>
#include <WiFi.h>
#include "Adafruit_MQTT.h"
#include "Adafruit_MQTT_Client.h"
#include <ESP32Servo.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "freertos/queue.h"
#include "freertos/semphr.h"

// ============================================================
// 1. CONFIGURATION & PIN DEFINITIONS
// ============================================================
#define WIFI_SSID       "Wokwi-GUEST"
#define WIFI_PASS       ""
#define IO_USERNAME     "Shashank_12"
#define IO_KEY          ""

#define PIN_POT_HR      34
#define PIN_POT_BP      35
#define PIN_POT_TEMP    32
#define PIN_POT_SPO2    33
#define PIN_POT_AQI     36

#define PIN_WIFI_BTN    15
#define PIN_BUZZER      13
#define PIN_LED_GREEN   12
#define PIN_LED_YELLOW  14
#define PIN_LED_RED     27
#define PIN_SERVO        5

#define BED_MIN          0
#define BED_MAX         90
#define BED_SLEEPING    10
#define BED_BREATHING   45
#define BED_EMERGENCY   90

#define DOSAGE_ALERT    80
#define RATE_MIN         5
#define RATE_MAX        60

#define STATE_ONLINE     0
#define STATE_DEGRADED   1
#define STATE_OFFLINE    2

// ============================================================
// 2. DATA STRUCTURES
// ============================================================
struct RawVitals {
  int hr;
  int bp;
  int temp;
  int spo2;
  int aqi;
  int roomTemp;
  int o2Level;
};

struct SystemData {
  RawVitals v;
  int dosage;
  int bedAngle;
  int samplingRate;
  int connection;
  int buffered;
  bool emergency;
};

struct TelemetryPacket {
  RawVitals v;
  int dosage;
  int bedAngle;
};

// ============================================================
// 3. HARDWARE & RTOS OBJECTS
// ============================================================
Servo bedServo;
Adafruit_SSD1306 display(128, 64, &Wire, -1);
WiFiClient client;
Adafruit_MQTT_Client mqtt(&client, "io.adafruit.com", 1883, IO_USERNAME, IO_KEY);

Adafruit_MQTT_Publish feedHR(&mqtt, IO_USERNAME "/feeds/heart-rate");
Adafruit_MQTT_Publish feedBP(&mqtt, IO_USERNAME "/feeds/sys-bp");
Adafruit_MQTT_Publish feedSpO2(&mqtt, IO_USERNAME "/feeds/spo2");
Adafruit_MQTT_Publish feedTemp(&mqtt, IO_USERNAME "/feeds/temperature");
Adafruit_MQTT_Publish feedRoomTemp(&mqtt, IO_USERNAME "/feeds/room-temp");
Adafruit_MQTT_Publish feedO2(&mqtt, IO_USERNAME "/feeds/oxygen-level");
Adafruit_MQTT_Publish feedAQI(&mqtt, IO_USERNAME "/feeds/aqi");

Adafruit_MQTT_Subscribe subDosage(&mqtt, IO_USERNAME "/feeds/dosage-slider");
Adafruit_MQTT_Subscribe subBed(&mqtt, IO_USERNAME "/feeds/bed-angle-slider");
Adafruit_MQTT_Subscribe subRate(&mqtt, IO_USERNAME "/feeds/sampling-rate");

QueueHandle_t healthQueue;
QueueHandle_t telemetryQueue;
SemaphoreHandle_t sensorMutex;
SemaphoreHandle_t i2cMutex;

RawVitals sharedVitals = {75, 120, 37, 98, 50, 24, 21};
volatile int currentDosage = 20;
volatile int manualBedAngle = BED_SLEEPING;
volatile int targetBedAngle = BED_SLEEPING;
volatile int actualBedAngle = BED_SLEEPING;
volatile int samplingRate = 10;
volatile int connectionState = STATE_OFFLINE;
volatile bool wifiEnabled = true;

// ============================================================
// 4. SENSOR SAMPLING (Task 1: Dedicated Tasks)
// ============================================================
void readSensorIndex(int idx) {
  int pins[] = {PIN_POT_HR, PIN_POT_BP, PIN_POT_TEMP, PIN_POT_SPO2, PIN_POT_AQI};
  int raw = analogRead(pins[idx]);

  xSemaphoreTake(sensorMutex, portMAX_DELAY);
  switch (idx) {
    case 0: sharedVitals.hr = map(raw, 0, 4095, 40, 180); break;
    case 1: sharedVitals.bp = map(raw, 0, 4095, 80, 200); break;
    case 2:
      sharedVitals.temp = map(raw, 0, 4095, 25, 42);
      sharedVitals.roomTemp = map(raw, 0, 4095, 18, 30);
      break;
    case 3:
      sharedVitals.spo2 = map(raw, 0, 4095, 70, 100);
      sharedVitals.o2Level = map(raw, 0, 4095, 19, 23);
      break;
    case 4: sharedVitals.aqi = map(raw, 0, 4095, 10, 400); break;
  }
  xSemaphoreGive(sensorMutex);
}

void TaskReadHR(void *p)   { for (;;) { readSensorIndex(0); vTaskDelay(pdMS_TO_TICKS(100)); } }
void TaskReadBP(void *p)   { for (;;) { readSensorIndex(1); vTaskDelay(pdMS_TO_TICKS(120)); } }
void TaskReadTemp(void *p) { for (;;) { readSensorIndex(2); vTaskDelay(pdMS_TO_TICKS(150)); } }
void TaskReadSpO2(void *p) { for (;;) { readSensorIndex(3); vTaskDelay(pdMS_TO_TICKS(100)); } }
void TaskReadAQI(void *p)  { for (;;) { readSensorIndex(4); vTaskDelay(pdMS_TO_TICKS(200)); } }

// ============================================================
// 5. COORDINATOR TASK
// ============================================================
void TaskCoordinator(void *p) {
  SystemData d;
  uint32_t lastSample = 0;

  for (;;) {
    xSemaphoreTake(sensorMutex, portMAX_DELAY);
    d.v = sharedVitals;
    xSemaphoreGive(sensorMutex);

    d.dosage = currentDosage;

    // Critical emergency evaluation
    d.emergency = (d.v.spo2 < 80) || (d.v.hr >= 130) || (d.dosage > DOSAGE_ALERT);

    // Dashboard sampling rate takes precedence unless active critical emergency
    if (d.emergency) {
      d.samplingRate = RATE_MIN; 
    } else {
      d.samplingRate = samplingRate;
    }

    // Dashboard manual bed angle takes precedence under normal/moderate vitals
    if (d.v.hr >= 140) {
      targetBedAngle = BED_EMERGENCY;  // 90°
    } else if (d.v.spo2 < 80) {
      targetBedAngle = BED_BREATHING;  // 45°
    } else {
      targetBedAngle = manualBedAngle; // Follows MQTT Slider Directly
    }

    d.bedAngle = actualBedAngle;
    d.connection = connectionState;
    d.buffered = uxQueueMessagesWaiting(telemetryQueue);

    xQueueOverwrite(healthQueue, &d);

    // Dynamic Telemetry Dispatch
    if (millis() - lastSample >= ((uint32_t)d.samplingRate * 1000UL)) {
      lastSample = millis();
      TelemetryPacket pkt = {d.v, d.dosage, d.bedAngle};

      if (uxQueueSpacesAvailable(telemetryQueue) == 0) {
        TelemetryPacket oldPkt;
        xQueueReceive(telemetryQueue, &oldPkt, 0);
      }
      xQueueSend(telemetryQueue, &pkt, 0);
    }

    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// ============================================================
// 6. BED SERVO CONTROLLER
// ============================================================
void TaskBedControl(void *p) {
  for (;;) {
    if (actualBedAngle < targetBedAngle) actualBedAngle++;
    else if (actualBedAngle > targetBedAngle) actualBedAngle--;

    actualBedAngle = constrain(actualBedAngle, BED_MIN, BED_MAX);
    bedServo.write(actualBedAngle);
    vTaskDelay(pdMS_TO_TICKS(25));
  }
}

// ============================================================
// 7. ALERTS TASK
// ============================================================
void TaskAlerts(void *p) {
  SystemData d;
  bool blink = false;

  for (;;) {
    if (xQueuePeek(healthQueue, &d, 0) == pdTRUE) {
      blink = !blink;

      if (d.dosage > DOSAGE_ALERT || d.emergency) {
        digitalWrite(PIN_LED_RED, blink ? HIGH : LOW);
        digitalWrite(PIN_LED_YELLOW, LOW);
        digitalWrite(PIN_LED_GREEN, LOW);
        tone(PIN_BUZZER, 2200);
      } else {
        noTone(PIN_BUZZER);
        digitalWrite(PIN_LED_RED, (d.connection == STATE_OFFLINE) ? (blink ? HIGH : LOW) : LOW);
        digitalWrite(PIN_LED_YELLOW, (d.connection == STATE_DEGRADED) ? (blink ? HIGH : LOW) : LOW);
        digitalWrite(PIN_LED_GREEN, (d.connection == STATE_ONLINE) ? HIGH : LOW);

        if (d.connection == STATE_OFFLINE && blink) {
          tone(PIN_BUZZER, 500, 100);
        }
      }
    }
    vTaskDelay(pdMS_TO_TICKS(100));
  }
}

// ============================================================
// 8. OLED DISPLAY TASK
// ============================================================
void TaskOLED(void *p) {
  SystemData d;

  for (;;) {
    if (xQueuePeek(healthQueue, &d, 0) == pdTRUE) {
      xSemaphoreTake(i2cMutex, portMAX_DELAY);

      display.clearDisplay();
      display.setTextColor(SSD1306_WHITE);
      display.setTextSize(1);
      display.setTextWrap(false);

      // Line 1: Connectivity
      display.setCursor(0, 0);
      if (d.connection == STATE_ONLINE) {
        display.print("=== ONLINE ===");
      } else if (d.connection == STATE_DEGRADED) {
        display.printf("DEGRADED BUF:%d", d.buffered);
      } else {
        display.printf("LOGGING OFFLINE %d", d.buffered);
      }

      // Line 2: Vitals
      display.setCursor(0, 12);
      display.printf("HR:%d BP:%d", d.v.hr, d.v.bp);

      // Line 3: Environment & Bed
      display.setCursor(0, 23);
      display.printf("SpO2:%d%% Bed:%d*", d.v.spo2, d.bedAngle);

      // Line 4: Rate Interval
      display.setCursor(0, 34);
      display.printf("Rate: %ds", d.samplingRate);

      // Line 5 & 6: Dosage
      display.setCursor(0, 45);
      display.print("Current Dosage:");
      display.setCursor(0, 55);
      display.printf("%d mg/hr", d.dosage);

      display.display();
      xSemaphoreGive(i2cMutex);
    }
    vTaskDelay(pdMS_TO_TICKS(200));
  }
}

// ============================================================
// 9. MQTT RECEIVE & PUBLISH
// ============================================================
void publishTelemetry(const TelemetryPacket &p) {
  static int fIdx = 0;
  switch (fIdx) {
    case 0: feedHR.publish((int32_t)p.v.hr); break;
    case 1: feedBP.publish((int32_t)p.v.bp); break;
    case 2: feedSpO2.publish((int32_t)p.v.spo2); break;
    case 3: feedTemp.publish((int32_t)p.v.temp); break;
    case 4: feedRoomTemp.publish((int32_t)p.v.roomTemp); break;
    case 5: feedO2.publish((int32_t)p.v.o2Level); break;
    case 6: feedAQI.publish((int32_t)p.v.aqi); break;
  }
  fIdx = (fIdx + 1) % 7;
}

void TaskMQTT(void *p) {
  uint32_t lastPub = 0;
  uint32_t lastPing = 0;

  for (;;) {
    if (!wifiEnabled || WiFi.status() != WL_CONNECTED) {
      connectionState = STATE_OFFLINE;
      vTaskDelay(pdMS_TO_TICKS(500));
      continue;
    }

    if (!mqtt.connected()) {
      connectionState = STATE_DEGRADED;
      if (mqtt.connect() != 0) {
        mqtt.disconnect();
        vTaskDelay(pdMS_TO_TICKS(3000));
        continue;
      }
      connectionState = STATE_ONLINE;
      mqtt.subscribe(&subDosage);
      mqtt.subscribe(&subBed);
      mqtt.subscribe(&subRate);
    }

    connectionState = STATE_ONLINE;

    // Process dashboard controls
    Adafruit_MQTT_Subscribe *sub;
    while ((sub = mqtt.readSubscription(50))) {
      int val = (int)atof((char *)sub->lastread);

      if (sub == &subDosage && val >= 0 && val <= 100) {
        currentDosage = val;
        Serial.printf("[DASHBOARD] Dosage: %d mg/hr\n", currentDosage);
      } else if (sub == &subBed && val >= BED_MIN && val <= BED_MAX) {
        manualBedAngle = val;
        Serial.printf("[DASHBOARD] Bed Angle: %d deg\n", manualBedAngle);
      } else if (sub == &subRate && val >= RATE_MIN && val <= RATE_MAX) {
        samplingRate = val;
        Serial.printf("[DASHBOARD] Sampling Rate: %d sec\n", samplingRate);
      }
    }

    if (millis() - lastPub >= 2500) {
      lastPub = millis();
      TelemetryPacket pkt;
      if (xQueueReceive(telemetryQueue, &pkt, 0) == pdTRUE) {
        publishTelemetry(pkt);
      }
    }

    if (millis() - lastPing >= 8000) {
      lastPing = millis();
      if (!mqtt.ping()) {
        mqtt.disconnect();
        connectionState = STATE_DEGRADED;
      }
    }

    vTaskDelay(pdMS_TO_TICKS(10));
  }
}

// ============================================================
// 10. NETWORK SIMULATION
// ============================================================
void TaskNetworkSim(void *p) {
  int lastBtn = HIGH;
  vTaskDelay(pdMS_TO_TICKS(3000));

  for (;;) {
    int btn = digitalRead(PIN_WIFI_BTN);
    if (btn == LOW && lastBtn == HIGH) {
      vTaskDelay(pdMS_TO_TICKS(50));
      wifiEnabled = !wifiEnabled;

      if (!wifiEnabled) {
        mqtt.disconnect();
        WiFi.disconnect(true);
        WiFi.mode(WIFI_OFF);
        connectionState = STATE_OFFLINE;
      } else {
        WiFi.mode(WIFI_STA);
        WiFi.begin(WIFI_SSID, WIFI_PASS);
      }
    }
    lastBtn = btn;
    vTaskDelay(pdMS_TO_TICKS(50));
  }
}

// ============================================================
// 11. SETUP & CORE ALLOCATION
// ============================================================
void setup() {
  Serial.begin(115200);

  pinMode(PIN_BUZZER, OUTPUT);
  pinMode(PIN_LED_GREEN, OUTPUT);
  pinMode(PIN_LED_YELLOW, OUTPUT);
  pinMode(PIN_LED_RED, OUTPUT);
  pinMode(PIN_WIFI_BTN, INPUT_PULLUP);

  // Allocate hardware PWM timers to avoid tone() clashes with servo
  ESP32PWM::allocateTimer(0);
  ESP32PWM::allocateTimer(1);
  ESP32PWM::allocateTimer(2);
  ESP32PWM::allocateTimer(3);

  bedServo.setPeriodHertz(50);
  bedServo.attach(PIN_SERVO, 500, 2400);
  bedServo.write(BED_SLEEPING);

  sensorMutex = xSemaphoreCreateMutex();
  i2cMutex = xSemaphoreCreateMutex();
  healthQueue = xQueueCreate(1, sizeof(SystemData));
  telemetryQueue = xQueueCreate(30, sizeof(TelemetryPacket));

  Wire.begin(21, 22);
  display.begin(SSD1306_SWITCHCAPVCC, 0x3C);

  WiFi.mode(WIFI_STA);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_SSID, WIFI_PASS);

  // Dedicated sensor reading tasks
  xTaskCreatePinnedToCore(TaskReadHR,   "T_HR",   2048, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(TaskReadBP,   "T_BP",   2048, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(TaskReadTemp, "T_Temp", 2048, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(TaskReadSpO2, "T_SpO2", 2048, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(TaskReadAQI,  "T_AQI",  2048, NULL, 3, NULL, 1);

  // Core management tasks
  xTaskCreatePinnedToCore(TaskCoordinator, "Coord", 4096, NULL, 3, NULL, 1);
  xTaskCreatePinnedToCore(TaskAlerts,      "Alert", 2048, NULL, 2, NULL, 1);
  xTaskCreatePinnedToCore(TaskBedControl,  "Bed",   2048, NULL, 2, NULL, 1);
  xTaskCreatePinnedToCore(TaskOLED,        "OLED",  4096, NULL, 1, NULL, 1);
  xTaskCreatePinnedToCore(TaskMQTT,        "MQTT",  8192, NULL, 1, NULL, 0);
  xTaskCreatePinnedToCore(TaskNetworkSim,  "Net",   2048, NULL, 1, NULL, 0);
}

void loop() {
  vTaskDelay(pdMS_TO_TICKS(1000));
}
