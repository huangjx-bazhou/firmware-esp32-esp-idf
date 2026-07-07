idf_component_register(SRCS "tcp_client_v4.c" "wifi_tcp_mdns.c"
                    PRIV_REQUIRES esp_wifi nvs_flash esp_netif
                    INCLUDE_DIRS ".")
