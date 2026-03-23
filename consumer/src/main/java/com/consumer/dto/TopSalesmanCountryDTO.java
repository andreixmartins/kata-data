package com.consumer.dto;

public class TopSalesmanCountryDTO {

    private String salesman;
    private String country;
    private Double totalAmount;

    public TopSalesmanCountryDTO(String salesman, String country, Double totalAmount) {
        this.salesman = salesman;
        this.country = country;
        this.totalAmount = totalAmount;
    }

    public String getSalesman() {
        return salesman;
    }

    public String getCountry() {
        return country;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }
}
