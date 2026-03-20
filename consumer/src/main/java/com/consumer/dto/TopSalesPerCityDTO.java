package com.consumer.dto;

public class TopSalesPerCityDTO {

    private String city;
    private Double totalAmount;

    public TopSalesPerCityDTO(String city, Double totalAmount) {
        this.city = city;
        this.totalAmount = totalAmount;
    }

    public String getCity() {
        return city;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }
}
