/*
 * SPDX-FileCopyrightText: 2022 Espressif Systems (Shanghai) CO LTD
 *
 * SPDX-License-Identifier: Unlicense OR CC0-1.0
 */
#include "sdkconfig.h"
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <errno.h>
#include <arpa/inet.h>
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"
#include "driver/uart.h"
#include "driver/uart_vfs.h"
#include "esp_check.h"
#include "esp_netif.h"
#include "esp_log.h"
#include "esp_vfs_dev.h"

static const char *TAG = "tcp client";

static const TickType_t reconnect_delay_ticks = pdMS_TO_TICKS(2000);
static const int select_wait_timeout_ms = 20;

#define BRIDGE_UART_PORT UART_NUM_0
// #define BRIDGE_UART_TX_PIN 25
// #define BRIDGE_UART_RX_PIN 24
#define BRIDGE_UART_BAUDRATE 460800
#define BRIDGE_RX_BUF_SIZE 1024

static bool s_uart_inited = false;
static int s_uart_fd = -1;

static void log_hex_data(const uint8_t *data, int len)
{
    char line[3 * 16 + 1] = {0};

    for (int i = 0; i < len; i += 16) {
        int chunk = (len - i) > 16 ? 16 : (len - i);
        int pos = 0;

        for (int j = 0; j < chunk; ++j) {
            pos += snprintf(&line[pos], sizeof(line) - pos, "%02X ", data[i + j]);
            if (pos >= (int)sizeof(line)) {
                break;
            }
        }

        // ESP_LOGI(TAG, "TCP RX HEX[%d:%d]: %s", i, i + chunk - 1, line);
    }
}

static esp_err_t bridge_uart_init(void)
{
    if (s_uart_inited) {
        return ESP_OK;
    }

    const uart_config_t uart_cfg = {
        .baud_rate = BRIDGE_UART_BAUDRATE,
        .data_bits = UART_DATA_8_BITS,
        .parity = UART_PARITY_DISABLE,
        .stop_bits = UART_STOP_BITS_1,
        .flow_ctrl = UART_HW_FLOWCTRL_DISABLE,
        .source_clk = UART_SCLK_DEFAULT,
    };

    ESP_RETURN_ON_ERROR(uart_param_config(BRIDGE_UART_PORT, &uart_cfg), TAG, "uart_param_config failed");
    // ESP_RETURN_ON_ERROR(uart_set_pin(BRIDGE_UART_PORT,
    //                                  BRIDGE_UART_TX_PIN,
    //                                  BRIDGE_UART_RX_PIN,
    //                                  UART_PIN_NO_CHANGE,
    //                                  UART_PIN_NO_CHANGE), TAG, "uart_set_pin failed");
    ESP_RETURN_ON_ERROR(uart_driver_install(BRIDGE_UART_PORT,
                                            BRIDGE_RX_BUF_SIZE * 2,
                                            BRIDGE_RX_BUF_SIZE * 2,
                                            0,
                                            NULL,
                                            0), TAG, "uart_driver_install failed");

    uart_vfs_dev_use_driver(BRIDGE_UART_PORT);

    //uart_vfs_dev_port_set_rx_line_endings(BRIDGE_UART_PORT, ESP_LINE_ENDINGS_CRLF);
    //uart_vfs_dev_port_set_tx_line_endings(BRIDGE_UART_PORT, ESP_LINE_ENDINGS_CRLF);

    char uart_path[16] = {0};
    int path_len = snprintf(uart_path, sizeof(uart_path), "/dev/uart/%d", (int)BRIDGE_UART_PORT);
    if (path_len <= 0 || path_len >= (int)sizeof(uart_path)) {
        // ESP_LOGE(TAG, "failed to build uart path");
        return ESP_FAIL;
    }

    s_uart_fd = open(uart_path, O_RDWR | O_NONBLOCK);
    if (s_uart_fd < 0) {
        // ESP_LOGE(TAG, "open %s failed: errno %d", uart_path, errno);
        return ESP_FAIL;
    }

    s_uart_inited = true;
    // ESP_LOGI(TAG, "uart bridge inited: port=%d baud=%d fd=%d",
    //          BRIDGE_UART_PORT,
    //          BRIDGE_UART_BAUDRATE,
    //          s_uart_fd);
    return ESP_OK;
}

static int send_all(int sock, const uint8_t *buf, int len)
{
    int sent = 0;
    while (sent < len) {
        int n = send(sock, buf + sent, len - sent, 0);
        if (n <= 0) {
            return -1;
        }
        sent += n;
    }
    return sent;
}

static void tcp_uart_bridge_loop(int sock)
{
    uint8_t tcp_rx_buf[BRIDGE_RX_BUF_SIZE];
    uint8_t uart_rx_buf[BRIDGE_RX_BUF_SIZE];

    while (1) {
        fd_set readfds;
        FD_ZERO(&readfds);
        FD_SET(sock, &readfds);
        FD_SET(s_uart_fd, &readfds);

        int max_fd = sock > s_uart_fd ? sock : s_uart_fd;

        struct timeval timeout = {
            .tv_sec = 0,
            .tv_usec = select_wait_timeout_ms * 1000,
        };

        int sel = select(max_fd + 1, &readfds, NULL, NULL, &timeout);
        if (sel < 0) {
            if (errno == EINTR) {
                continue;
            }
            // ESP_LOGE(TAG, "select failed: errno %d", errno);
            break;
        }

        if (sel <= 0) {
            continue;
        }

        /* TCP has priority over UART when both are ready. */
        if (FD_ISSET(sock, &readfds)) {
            int tcp_len = recv(sock, tcp_rx_buf, sizeof(tcp_rx_buf), 0);
            if (tcp_len > 0) {
                // ESP_LOGI(TAG, "tcp rx %d bytes", tcp_len);
                // log_hex_data(tcp_rx_buf, tcp_len);

                int written = uart_write_bytes(BRIDGE_UART_PORT, tcp_rx_buf, tcp_len);
                if (written < 0) {
                    // ESP_LOGE(TAG, "uart_write_bytes failed");
                    break;
                }
                continue;
            }

            if (tcp_len == 0) {
                // ESP_LOGW(TAG, "server closed connection");
            } else {
                // ESP_LOGE(TAG, "recv failed: errno %d", errno);
            }
            break;
        }

        if (FD_ISSET(s_uart_fd, &readfds)) {
            int uart_len = read(s_uart_fd, uart_rx_buf, sizeof(uart_rx_buf));
            //int uart_len = uart_read_bytes(UART_NUM_0, uart_rx_buf, sizeof(uart_rx_buf), pdMS_TO_TICKS(100));

            if (uart_len > 0) {
                if (send_all(sock, uart_rx_buf, uart_len) < 0) {
                    // ESP_LOGE(TAG, "send failed: errno %d", errno);
                    break;
                }
            } else if (uart_len < 0 && errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                // ESP_LOGE(TAG, "uart read failed: errno %d", errno);
                break;
            }
        }
    }
}

void tcp_client(void)
{
    char host_ip[] = CONFIG_ESP_SERVER_IP;
    int addr_family = 0;
    int ip_protocol = 0;

    if (bridge_uart_init() != ESP_OK) {
        // ESP_LOGE(TAG, "uart init failed, tcp client stopped");
        return;
    }

    while (1) {
        struct sockaddr_in dest_addr = {0};
        inet_pton(AF_INET, host_ip, &dest_addr.sin_addr);
        dest_addr.sin_family = AF_INET;
        dest_addr.sin_port = htons(CONFIG_ESP_SERVER_PORT);
        addr_family = AF_INET;
        ip_protocol = IPPROTO_IP;

        int sock =  socket(addr_family, SOCK_STREAM, ip_protocol);
        if (sock < 0) {
            // ESP_LOGE(TAG, "Unable to create socket: errno %d", errno);
            vTaskDelay(reconnect_delay_ticks);
            continue;
        }

        // ESP_LOGI(TAG, "Socket created, connecting to %s:%d", host_ip, CONFIG_ESP_SERVER_PORT);

        int err = connect(sock, (struct sockaddr *)&dest_addr, sizeof(dest_addr));
        if (err != 0) {
            // ESP_LOGE(TAG, "Socket unable to connect: errno %d", errno);
            shutdown(sock, 0);
            close(sock);
            vTaskDelay(reconnect_delay_ticks);
            continue;
        }
        // ESP_LOGI(TAG, "Successfully connected");

        tcp_uart_bridge_loop(sock);

        if (sock != -1) {
            // ESP_LOGE(TAG, "Shutting down socket and restarting...");
            shutdown(sock, 0);
            close(sock);
        }

        vTaskDelay(reconnect_delay_ticks);
    }
}
