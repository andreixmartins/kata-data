package com.consumer.dto;

public class TopSalesmanCountryDTO {

    private String salesman;
    private Double totalAmount;

    public TopSalesmanCountryDTO(String salesman, Double totalAmount) {
        this.salesman = salesman;
        this.totalAmount = totalAmount;
    }

    public String getSalesman() {
        return salesman;
    }

    public Double getTotalAmount() {
        return totalAmount;
    }
}
