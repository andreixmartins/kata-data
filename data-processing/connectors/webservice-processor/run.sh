#!/bin/bash
set -e

CONNECT_URL="http://localhost:8083"
SOAP_URL="http://localhost:8181/sellers"

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

SSNS=("123-45-6789" "123-98-7654" "222-33-4444" "321-54-9876" "444-55-6666" "555-66-7777" "111-22-3333" "234-56-7890" "777-66-5555" "888-77-6666" "999-88-7777")
UUIDS=("550e8400-e29b-41d4-a716-446655440000" "550e8400-e29b-41d4-a716-446655440001" "550e8400-e29b-41d4-a716-446655440002" "550e8400-e29b-41d4-a716-446655440003" "550e8400-e29b-41d4-a716-446655440004" "550e8400-e29b-41d4-a716-446655440005" "550e8400-e29b-41d4-a716-446655440006" "550e8400-e29b-41d4-a716-446655440007" "550e8400-e29b-41d4-a716-446655440008" "550e8400-e29b-41d4-a716-446655440009" "550e8400-e29b-41d4-a716-446655440010")
SELLERS=("Alice Johnson" "Bob Smith" "Carlos Garcia" "Diana Tanaka" "Erik Mueller" "Fernanda Costa" "Grace Kim" "Hiro Nakamura" "Ivan Petrov" "Julia Santos" "Klaus Weber")
CITIES=("New York" "London" "Madrid" "Tokyo" "Berlin" "Sao Paulo" "Seoul" "Osaka" "Moscow" "Lisbon" "Vienna")
COUNTRIES=("US" "UK" "ES" "JP" "DE" "BR" "KR" "JP" "RU" "PT" "AT")
AMOUNTS=("4500.00" "3200.50" "7800.00" "1250.75" "5600.00" "9100.25" "2750.00" "4100.80" "6300.00" "1890.50" "3450.75")
CURRENCIES=("USD" "GBP" "EUR" "JPY" "EUR" "BRL" "KRW" "JPY" "RUB" "EUR" "EUR")
DATES=("2026-01-20" "2026-01-25" "2026-02-10" "2026-02-20" "2026-03-10" "2026-03-20" "2026-01-15" "2026-02-05" "2026-02-28" "2026-03-05" "2026-03-15")

for i in $(seq 0 10); do
    echo "Sending seller request ${UUIDS[$i]}..."
    curl -s -X POST "$SOAP_URL" \
        -H "Content-Type: text/xml; charset=utf-8" \
        -H "SOAPAction: http://sales.com/submitSeller" \
        -d "<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ws=\"http://sales.com/wsdl\">
   <soapenv:Header/>
   <soapenv:Body>
      <ws:SubmitSellerRequest>
         <ws:ssn>${SSNS[$i]}</ws:ssn>
         <ws:saleId>${UUIDS[$i]}</ws:saleId>
         <ws:sellerName>${SELLERS[$i]}</ws:sellerName>
         <ws:city>${CITIES[$i]}</ws:city>
         <ws:country>${COUNTRIES[$i]}</ws:country>
         <ws:totalAmount>${AMOUNTS[$i]}</ws:totalAmount>
         <ws:currency>${CURRENCIES[$i]}</ws:currency>
         <ws:saleDate>${DATES[$i]}</ws:saleDate>
      </ws:SubmitSellerRequest>
   </soapenv:Body>
</soapenv:Envelope>"
    echo ""
done

echo "=== Connector Status ==="
curl -s "$CONNECT_URL/connectors/sellers-webservice-source/status" | grep -o '"state":"[^"]*"'

echo ""
echo "=== Messages on sales.raw.sellers.webservice.v1 ==="
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 \
    --topic sales.raw.sellers.webservice.v1 \
    --from-beginning \
    --timeout-ms 5000 \
    --property print.key=true 2>/dev/null || true
