#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
SOAP_URL="http://localhost:8181/shipping"

echo "Waiting for SOAP endpoint on port 8181..."
for i in $(seq 1 60); do
    if curl -s -o /dev/null -w "%{http_code}" "${SOAP_URL}?wsdl" 2>/dev/null | grep -q "200"; then
        echo "SOAP endpoint ready"
        break
    fi
    if [ "$i" -eq 60 ]; then
        echo "SOAP endpoint not available after 60s"
        exit 1
    fi
    sleep 1
done

UUIDS=("550e8400-e29b-41d4-a716-446655440000" "550e8400-e29b-41d4-a716-446655440001" "550e8400-e29b-41d4-a716-446655440002" "550e8400-e29b-41d4-a716-446655440003" "550e8400-e29b-41d4-a716-446655440004" "550e8400-e29b-41d4-a716-446655440005")
METHODS=("express" "standard" "express" "international" "standard" "international")
TRACKING=("TRK-12345" "TRK-12346" "TRK-12347" "TRK-12348" "TRK-12349" "TRK-12350")
ADDRESSES=("123 Main St, New York, NY" "45 Baker St, London, UK" "Calle Gran Via 10, Madrid, ES" "1-2-3 Shibuya, Tokyo, JP" "Unter den Linden 5, Berlin, DE" "Av Paulista 1000, Sao Paulo, BR")
DELIVERIES=("2026-01-20" "2026-01-25" "2026-02-10" "2026-02-20" "2026-03-10" "2026-03-20")

for i in $(seq 0 5); do
    echo "Sending shipping request ${UUIDS[$i]}..."
    curl -s -X POST "$SOAP_URL" \
        -H "Content-Type: text/xml; charset=utf-8" \
        -H "SOAPAction: http://sales.com/submitShipping" \
        -d "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ws=\"http://sales.com/wsdl\">
   <soapenv:Header/>
   <soapenv:Body>
      <ws:SubmitShippingRequest>
         <ws:saleId>${UUIDS[$i]}</ws:saleId>
         <ws:shippingMethod>${METHODS[$i]}</ws:shippingMethod>
         <ws:trackingNumber>${TRACKING[$i]}</ws:trackingNumber>
         <ws:deliveryAddress>${ADDRESSES[$i]}</ws:deliveryAddress>
         <ws:estimatedDelivery>${DELIVERIES[$i]}</ws:estimatedDelivery>
      </ws:SubmitShippingRequest>
   </soapenv:Body>
</soapenv:Envelope>"
    echo ""
done

echo "=== Connector Status ==="
curl -s "$CONNECT_URL/connectors/shipping-webservice-source/status" | grep -o '"state":"[^"]*"'

echo ""
echo "=== Messages on sales.raw.shipping.webservice.v1 ==="
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic sales.raw.shipping.webservice.v1 \
    --from-beginning \
    --timeout-ms 5000 \
    --property print.key=true 2>/dev/null || true
