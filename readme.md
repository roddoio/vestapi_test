Prueba Vest

Repos de Códigos y URL de Kafka
-API --> https://github.com/roddoio/vestapi_test
-Servicio --> https://github.com/roddoio/vestservice_test
-BD y Docker compose file para deploy --> https://github.com/roddoio/vestdeploy_test
-Kafka --> https://confluent.cloud/environments/env-6359xq/clusters/lkc-pkmpzy/settings/kafka
		  Bootstrap server: pkc-lgk0v.us-west1.gcp.confluent.cloud:9092
		  REST Endpoint: https://pkc-lgk0v.us-west1.gcp.confluent.cloud:443

La documentación de la API se puede ver al cargar los contenedores e ingresar a GraphiQL http://localhost:8000/

Basicamente se realizaron 5 endpoints:

-buySymbol --> Compra de Stocks
-sellSymbol --> Venta de stocks
-stocksByUser --> Obtener los stocks de un usuario
-resumeStocksByUser --> Obtener el resumen de los stocks de un usuario
-historicPriceByStock --> Obtener el historico de precios de un usuario

Instrucciones:
1.- Descargar el repo https://github.com/roddoio/vestdeploy_test que contiene el archivo docker compose y el script inicial de la BD
2.- Ejecutar Docker compose : docker compose up -d