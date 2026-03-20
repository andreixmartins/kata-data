package com.consumer.controller;

import com.consumer.dto.TopSalesPerCityDTO;
import com.consumer.dto.TopSalesmanCountryDTO;
import com.consumer.entity.ResultEntity;
import com.consumer.service.ResultService;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/results")
public class ResultController {

    private final ResultService service;

    public ResultController(ResultService service) {
        this.service = service;
    }

    @GetMapping
    public List<ResultEntity> getAll() {
        return service.findAll();
    }

    @GetMapping("/top-salesman-country")
    public List<TopSalesmanCountryDTO> getTopSalesmanCountry() {
        return service.getTopSalesmanCountry();
    }

    @GetMapping("/top-sales-per-city")
    public List<TopSalesPerCityDTO> getTopSalesPerCity() {
        return service.getTopSalesPerCity();
    }
}
