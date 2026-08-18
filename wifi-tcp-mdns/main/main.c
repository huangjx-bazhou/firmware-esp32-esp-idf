#include "esp_system.h"
#include <arpa/inet.h>
#include <driver/uart.h>
#include <esp_event.h>
#include <esp_log.h>
#include <esp_wifi.h>
#include <freertos/event_groups.h>
#include <freertos/queue.h>
#include <freertos/ringbuf.h>
#include <freertos/task.h>
#include <hal/uart_types.h>
#include <nvs_flash.h>
#include <stddef.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define EXAMPLE_ESP_WIFI_SSID CONFIG_ESP_WIFI_SSID
#define EXAMPLE_ESP_WIFI_PASS CONFIG_ESP_WIFI_PASSWORD
#define EXAMPLE_ESP_MAXIMUM_RETRY CONFIG_ESP_MAXIMUM_RETRY

#if CONFIG_ESP_STATION_EXAMPLE_WPA3_SAE_PWE_HUNT_AND_PECK
#define ESP_WIFI_SAE_MODE WPA3_SAE_PWE_HUNT_AND_PECK
#define EXAMPLE_H2E_IDENTIFIER ""
#elif CONFIG_ESP_STATION_EXAMPLE_WPA3_SAE_PWE_HASH_TO_ELEMENT
#define ESP_WIFI_SAE_MODE WPA3_SAE_PWE_HASH_TO_ELEMENT
#define EXAMPLE_H2E_IDENTIFIER CONFIG_ESP_WIFI_PW_ID
#elif CONFIG_ESP_STATION_EXAMPLE_WPA3_SAE_PWE_BOTH
#define ESP_WIFI_SAE_MODE WPA3_SAE_PWE_BOTH
#define EXAMPLE_H2E_IDENTIFIER CONFIG_ESP_WIFI_PW_ID
#endif
#if CONFIG_ESP_WIFI_AUTH_OPEN
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_OPEN
#elif CONFIG_ESP_WIFI_AUTH_WEP
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WEP
#elif CONFIG_ESP_WIFI_AUTH_WPA_PSK
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WPA_PSK
#elif CONFIG_ESP_WIFI_AUTH_WPA2_PSK
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WPA2_PSK
#elif CONFIG_ESP_WIFI_AUTH_WPA_WPA2_PSK
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WPA_WPA2_PSK
#elif CONFIG_ESP_WIFI_AUTH_WPA3_PSK
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WPA3_PSK
#elif CONFIG_ESP_WIFI_AUTH_WPA2_WPA3_PSK
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WPA2_WPA3_PSK
#elif CONFIG_ESP_WIFI_AUTH_WAPI_PSK
#define ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD WIFI_AUTH_WAPI_PSK
#endif

#define UART_RX_BUF_SIZE (1024 * 8) // 8KB的UART接收缓冲区
#define UART_TX_BUF_SIZE (1024 * 8) // 8KB的UART发送缓冲区
#define UART_EVT_QUEUE_SIZE 32
#define UART_EVT_TASK_STACK 4096
#define UART_EVT_TASK_PRIO 9
#define UART_TX_TASK_STACK 4096
#define UART_TX_TASK_PRIO 9
#define UART_TCP_RINGBUF_SIZE (1024 * 16) // 16KB的环形缓冲区
#define TCP_CONNECT_TASK_STACK 4096
#define TCP_CONNECT_TASK_PRIO 8
#define TCP_RETRY_DELAY_MS 2000
#define TCP_CONNECT_TRIGGER_BIT BIT0
#define TCP_SEND_TASK_STACK 4096
#define TCP_SEND_TASK_PRIO 8
#define TCP_RECV_TASK_STACK 4096
#define TCP_RECV_TASK_PRIO 8
#define TCP_SOCKET_QUEUE_LEN 1
#define TCP_UART_RINGBUF_SIZE (1024 * 1) // 1KB的环形缓冲区

static QueueHandle_t uart_event_queue = NULL;
static RingbufHandle_t uart_tcp_ringbuf = NULL;
static RingbufHandle_t tcp_uart_ringbuf = NULL;
static TaskHandle_t uart_tx_task_handle = NULL;
static TaskHandle_t tcp_connect_task_handle = NULL;
static TaskHandle_t tcp_send_task_handle = NULL;
static TaskHandle_t tcp_recv_task_handle = NULL;
static EventGroupHandle_t tcp_connect_event_group = NULL;
static QueueHandle_t tcp_send_socket_queue = NULL;
static QueueHandle_t tcp_recv_socket_queue = NULL;

/**
 * @brief UART事件处理任务
 * @param arg 任务参数
 */
static void uart_event_task(void *arg) {
  (void)arg;
  uart_event_t event;
  uint8_t data[512];

  while (1) {
    // 等待UART事件队列中的事件
    if (pdTRUE != xQueueReceive(uart_event_queue, &event, portMAX_DELAY)) {
      continue;
    }

    // 处理UART事件
    switch (event.type) {
    case UART_DATA: { // Rx Ring Buffer 有数据到来
      // 接收缓冲区中的数据个数
      size_t remain = event.size;

      // 循环读取
      while (remain > 0) {
        size_t to_read = remain > sizeof(data) ? sizeof(data) : remain;
        int len = uart_read_bytes(UART_NUM_0, data, to_read, 0);
        if (len <= 0) {
          break;
        }
        xRingbufferSend(uart_tcp_ringbuf, data, len, 0);
        remain -= len;
      }
      break;
    }
    case UART_FIFO_OVF:    // Rx FIFO溢出
    case UART_BUFFER_FULL: // Rx Ring Buffer满
      // 如果硬件FIFO溢出或接收缓冲区满，等待发送完毕，清空UART的缓冲区，这时接收缓冲区数据丢失
      uart_wait_tx_done(UART_NUM_0, pdMS_TO_TICKS(100));
      uart_flush(UART_NUM_0);
      break;
    default:
      break;
    }
  }
}

/**
 * @brief UART发送任务
 * @param arg 任务参数
 */
static void uart_tx_task(void *arg) {
  (void)arg;

  while (1) {
    size_t item_len = 0;
    uint8_t *item = (uint8_t *)xRingbufferReceiveUpTo(
        tcp_uart_ringbuf, &item_len, portMAX_DELAY, 512);
    if (NULL == item) {
      continue;
    }

    size_t written = 0;
    while (written < item_len) {
      int n = uart_write_bytes(UART_NUM_0, (const char *)item + written,
                               item_len - written);
      if (n <= 0) {
        break;
      }
      written += (size_t)n;
    }

    vRingbufferReturnItem(tcp_uart_ringbuf, item);
  }
}

/*
 * @brief 初始化UART
 */
static void uart_init() {
  // 安装驱动程序并启用事件队列
  ESP_ERROR_CHECK(uart_driver_install(UART_NUM_0, UART_RX_BUF_SIZE,
                                      UART_TX_BUF_SIZE, UART_EVT_QUEUE_SIZE,
                                      &uart_event_queue, 0));

  // 配置UART参数
  uart_config_t uart_config = {.baud_rate = 460800,
                               .data_bits = UART_DATA_8_BITS,
                               .parity = UART_PARITY_DISABLE,
                               .stop_bits = UART_STOP_BITS_1,
                               .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
                               .rx_flow_ctrl_thresh = 0,
                               .source_clk = UART_SCLK_DEFAULT,
                               .flags = {.allow_pd = 1}};

  ESP_ERROR_CHECK(uart_param_config(UART_NUM_0, &uart_config));

  if (xTaskCreate(uart_event_task, "uart_event_task", UART_EVT_TASK_STACK, NULL,
                  UART_EVT_TASK_PRIO, NULL) != pdPASS) {
    esp_restart();
  }

  if (xTaskCreate(uart_tx_task, "uart_tx_task", UART_TX_TASK_STACK, NULL,
                  UART_TX_TASK_PRIO, &uart_tx_task_handle) != pdPASS) {
    uart_tx_task_handle = NULL;
    esp_restart();
  }
}

/**
 * @brief TCP发送任务
 */
static void tcp_send_task(void *arg) {
  (void)arg;
  int sock = -1;

  while (1) {
    // 等待socket描述符从队列中发送过来
    if (pdTRUE != xQueueReceive(tcp_send_socket_queue, &sock, portMAX_DELAY)) {
      continue;
    }

    while (1) {
      size_t item_len = 0;
      uint8_t *item = (uint8_t *)xRingbufferReceiveUpTo(
          uart_tcp_ringbuf, &item_len, portMAX_DELAY, 512);
      if (NULL == item) {
        continue;
      }

      size_t sent = 0;
      while (sent < item_len) {
        int n = send(sock, item + sent, item_len - sent, 0);
        if (n <= 0) {
          vRingbufferReturnItem(uart_tcp_ringbuf, item);
          shutdown(sock, SHUT_RDWR);
          close(sock);
          sock = -1;
          goto wait_next_socket;
        }
        sent += n;
      }

      vRingbufferReturnItem(uart_tcp_ringbuf, item);
    }

  wait_next_socket:;
  }
}

/**
 * @brief TCP接收任务
 */
static void tcp_recv_task(void *arg) {
  (void)arg;
  int sock = -1;
  uint8_t data[512];

  while (1) {
    if (pdTRUE != xQueueReceive(tcp_recv_socket_queue, &sock, portMAX_DELAY)) {
      continue;
    }

    while (1) {
      int len = recv(sock, data, sizeof(data), 0);

      ESP_LOGI(__func__, "TCP received data, sock=%d, len=%d", sock, len);

      if (len <= 0) {
        shutdown(sock, SHUT_RDWR);
        close(sock);
        sock = -1;
        xEventGroupSetBits(tcp_connect_event_group, TCP_CONNECT_TRIGGER_BIT);
        break;
      }

      xRingbufferSend(tcp_uart_ringbuf, data, (size_t)len, 0);
    }
  }
}

/**
 * @brief TCP连接任务
 */
static void tcp_connect_task(void *arg) {
  (void)arg;

  while (1) {
    xEventGroupWaitBits(tcp_connect_event_group, TCP_CONNECT_TRIGGER_BIT,
                        pdTRUE, pdFALSE, portMAX_DELAY);

    ESP_LOGI(__func__, "TCP connect triggered");

    // 循环创建，连接
    while (1) {
      struct sockaddr_in dest_addr = {0};
      dest_addr.sin_family = AF_INET;
      dest_addr.sin_port = htons(CONFIG_ESP_SERVER_PORT);
      if (inet_pton(AF_INET, CONFIG_ESP_SERVER_IP, &dest_addr.sin_addr) != 1) {
        vTaskDelay(pdMS_TO_TICKS(TCP_RETRY_DELAY_MS));
        continue;
      }

      int sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
      if (sock < 0) {
        vTaskDelay(pdMS_TO_TICKS(TCP_RETRY_DELAY_MS));
        continue;
      }

      ESP_LOGI(__func__, "Created socket, connecting to %s:%d",
               CONFIG_ESP_SERVER_IP, CONFIG_ESP_SERVER_PORT);

      int err = connect(sock, (struct sockaddr *)&dest_addr, sizeof(dest_addr));

      if (0 == err) {
        ESP_LOGI(__func__, "TCP connected, sock=%d", sock);

        int send_sock = sock;
        int recv_sock = sock;

        xQueueReset(tcp_send_socket_queue);
        xQueueReset(tcp_recv_socket_queue);

        if (pdTRUE != xQueueSend(tcp_send_socket_queue, &send_sock,
                                 pdMS_TO_TICKS(1000))) {
          shutdown(sock, SHUT_RDWR);
          close(sock);
          vTaskDelay(pdMS_TO_TICKS(TCP_RETRY_DELAY_MS));
          continue;
        }

        if (pdTRUE != xQueueSend(tcp_recv_socket_queue, &recv_sock,
                                 pdMS_TO_TICKS(1000))) {
          shutdown(sock, SHUT_RDWR);
          close(sock);
          vTaskDelay(pdMS_TO_TICKS(TCP_RETRY_DELAY_MS));
          continue;
        }

        break;
      }

      shutdown(sock, SHUT_RDWR);
      close(sock);

      // 打印连接失败原因
      ESP_LOGW(__func__, "TCP connect failed, sock=%d, err=%d", sock, errno);

      // 连接失败，等待一段时间后重试
      vTaskDelay(pdMS_TO_TICKS(TCP_RETRY_DELAY_MS));
    }
  }
}

/**
 * @brief WIFI事件处理函数
 */
static void wifi_event_handler(void *arg, esp_event_base_t event_base,
                               int32_t event_id, void *event_data) {
  switch (event_id) {
  case WIFI_EVENT_STA_START:
    esp_wifi_connect();
    break;
  case WIFI_EVENT_STA_CONNECTED:
    ESP_LOGI(__func__, "connected to ap");
    /// TODO: 通知MCU连接AP成功
    break;
  case WIFI_EVENT_STA_DISCONNECTED:
    wifi_event_sta_disconnected_t *sta_disconnected_event_data =
        (wifi_event_sta_disconnected_t *)event_data;

    ESP_LOGI(__func__, "disconnected from ap, reason: %d",
             sta_disconnected_event_data->reason);
    /// TODO: 通知MCU已断开连接

    esp_wifi_connect();

    /// TODO: 根据sta_disconnected_event_data->reason判断是否需要重连
    switch (sta_disconnected_event_data->reason) {
    default:
      break;
    }
    break;
  case WIFI_EVENT_STA_STOP:
    break;
  case WIFI_EVENT_STA_BEACON_OFFSET_UNSTABLE:
    wifi_event_sta_beacon_offset_unstable_t
        *sta_beacon_offset_unstable_event_data =
            (wifi_event_sta_beacon_offset_unstable_t *)event_data;
    ESP_LOGI(__func__, "unstable sample, beacon success rate: %.4f",
             sta_beacon_offset_unstable_event_data->beacon_success_rate);
#if CONFIG_ESP_WIFI_SLP_SAMPLE_BEACON_FEATURE
    esp_wifi_beacon_offset_sample_beacon();
#endif
    break;
  default:
    break;
  }
}

/**
 * @brief STA获取IP地址事件处理函数
 */
static void sta_got_ip_handler(void *arg, esp_event_base_t event_base,
                               int32_t event_id, void *event_data) {
  /// TODO: 通知MCU已获取IP地址

  ip_event_got_ip_t *got_ip_event_data = (ip_event_got_ip_t *)event_data;
  ESP_LOGI(__func__, "ip_changed: %d", got_ip_event_data->ip_changed);

  xEventGroupSetBits(tcp_connect_event_group, TCP_CONNECT_TRIGGER_BIT);
}

/**
 * @brief STA丢失IP地址事件处理函数
 */
static void sta_lost_ip_handler(void *arg, esp_event_base_t event_base,
                                int32_t event_id, void *event_data) {
  ESP_LOGI(__func__, "lost address");
  xEventGroupClearBits(tcp_connect_event_group, TCP_CONNECT_TRIGGER_BIT);
  /// TODO: 通知MCU已丢失IP地址
}

/**
 * @brief 初始化STA模式的WiFi
 */
static void wifi_init_sta(void) {
  // If you want to open more logs in the wifi module, you need to make
  // the max level greater than the default level, and call
  // esp_log_level_set() before esp_wifi_init() to improve the log level of
  // the wifi module.
  if (CONFIG_LOG_MAXIMUM_LEVEL > CONFIG_LOG_DEFAULT_LEVEL) {
    esp_log_level_set("wifi", CONFIG_LOG_MAXIMUM_LEVEL);
  }

  ESP_ERROR_CHECK(esp_netif_init());

  esp_netif_t *sta_netif = esp_netif_create_default_wifi_sta();

  if (NULL == sta_netif) {
    esp_restart();
  }

  wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
  ESP_ERROR_CHECK(esp_wifi_init(&cfg));

  // Register event handlers
  esp_event_handler_instance_t instance_wifi_event_any_id;
  esp_event_handler_instance_t instance_ip_event_sta_got_ip;
  esp_event_handler_instance_t instance_ip_event_sta_lost_ip;
  ESP_ERROR_CHECK(esp_event_handler_instance_register(
      WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL,
      &instance_wifi_event_any_id));
  ESP_ERROR_CHECK(esp_event_handler_instance_register(
      IP_EVENT, IP_EVENT_STA_GOT_IP, &sta_got_ip_handler, NULL,
      &instance_ip_event_sta_got_ip));
  ESP_ERROR_CHECK(esp_event_handler_instance_register(
      IP_EVENT, IP_EVENT_STA_LOST_IP, &sta_lost_ip_handler, NULL,
      &instance_ip_event_sta_lost_ip));

  wifi_config_t wifi_config = {
      .sta =
          {
              .ssid = EXAMPLE_ESP_WIFI_SSID,
              .password = EXAMPLE_ESP_WIFI_PASS,
              /* Authmode threshold resets to WPA2 as default if password
               * matches WPA2 standards (password len => 8). If you want to
               * connect the device to deprecated WEP/WPA networks, Please set
               * the threshold value to WIFI_AUTH_WEP/WIFI_AUTH_WPA_PSK and set
               * the password with length and format matching to
               * WIFI_AUTH_WEP/WIFI_AUTH_WPA_PSK standards.
               */
              .threshold.authmode = ESP_WIFI_SCAN_AUTH_MODE_THRESHOLD,
              .sae_pwe_h2e = ESP_WIFI_SAE_MODE,
              .sae_h2e_identifier = EXAMPLE_H2E_IDENTIFIER,
#ifdef CONFIG_ESP_WIFI_WPA3_COMPATIBLE_SUPPORT
              .disable_wpa3_compatible_mode = 0,
#endif
          },
  };
  ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
  ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));

  // Create event group for TCP connection task
  tcp_connect_event_group = xEventGroupCreate();
  if (NULL == tcp_connect_event_group) {
    esp_restart();
  }

  // Create TCP send socket queue
  tcp_send_socket_queue = xQueueCreate(TCP_SOCKET_QUEUE_LEN, sizeof(int));
  if (NULL == tcp_send_socket_queue) {
    esp_restart();
  }

  // Create TCP receive socket queue
  tcp_recv_socket_queue = xQueueCreate(TCP_SOCKET_QUEUE_LEN, sizeof(int));
  if (NULL == tcp_recv_socket_queue) {
    esp_restart();
  }

  // Create TCP send task
  if (pdPASS != xTaskCreate(tcp_send_task, "tcp_send_task", TCP_SEND_TASK_STACK,
                            NULL, TCP_SEND_TASK_PRIO, &tcp_send_task_handle)) {
    tcp_send_task_handle = NULL;
    esp_restart();
  }

  // Create TCP receive task
  if (pdPASS != xTaskCreate(tcp_recv_task, "tcp_recv_task", TCP_RECV_TASK_STACK,
                            NULL, TCP_RECV_TASK_PRIO, &tcp_recv_task_handle)) {
    tcp_recv_task_handle = NULL;
    esp_restart();
  }

  // Create TCP connection task
  if (pdPASS != xTaskCreate(tcp_connect_task, "tcp_connect_task",
                            TCP_CONNECT_TASK_STACK, NULL, TCP_CONNECT_TASK_PRIO,
                            &tcp_connect_task_handle)) {
    tcp_connect_task_handle = NULL;
    esp_restart();
  }

  // Start WiFi
  ESP_ERROR_CHECK(esp_wifi_start());
}

void app_main(void) {
  // Initialize NVS
  esp_err_t ret = nvs_flash_init();
  if (ESP_ERR_NVS_NO_FREE_PAGES == ret ||
      ESP_ERR_NVS_NEW_VERSION_FOUND == ret) {
    ESP_ERROR_CHECK(nvs_flash_erase());
    ret = nvs_flash_init();
  }
  ESP_ERROR_CHECK(ret);

  // Configure dynamic frequency scaling:
  // maximum and minimum frequencies are set in sdkconfig,
  // automatic light sleep is enabled if tickless idle support is enabled.
#if CONFIG_PM_ENABLE & CONFIG_FREERTOS_USE_TICKLESS_IDLE
  esp_pm_config_t pm_config;
  if (ESP_OK == esp_pm_get_configuration(&pm_config)) {
    pm_config.light_sleep_enable = true;
    esp_pm_configure(&pm_config);
  }
#endif // CONFIG_PM_ENABLE & CONFIG_FREERTOS_USE_TICKLESS_IDLE

  // Initialize default event loop (shared by all components)
  ESP_ERROR_CHECK(esp_event_loop_create_default());

  uart_tcp_ringbuf =
      xRingbufferCreate(UART_TCP_RINGBUF_SIZE, RINGBUF_TYPE_BYTEBUF);
  if (NULL == uart_tcp_ringbuf) {
    esp_restart();
  }

  tcp_uart_ringbuf =
      xRingbufferCreate(TCP_UART_RINGBUF_SIZE, RINGBUF_TYPE_BYTEBUF);
  if (NULL == tcp_uart_ringbuf) {
    esp_restart();
  }

  // Initialize UART
  uart_init();

  // Initialize WiFi in STA mode
  wifi_init_sta();
}
