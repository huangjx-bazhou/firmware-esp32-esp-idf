idf_component_register(SRCS "wifi_tcp_mdns.c" "tcp_client_v4.c"
                    PRIV_REQUIRES esp_wifi nvs_flash esp_netif esp_driver_uart
                    INCLUDE_DIRS ".")
