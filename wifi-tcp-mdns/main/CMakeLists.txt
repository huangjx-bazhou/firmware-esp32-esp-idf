idf_component_register(SRCS "main.c"
        PRIV_REQUIRES esp_wifi nvs_flash esp_netif esp_driver_uart esp_ringbuf
        INCLUDE_DIRS ".")
