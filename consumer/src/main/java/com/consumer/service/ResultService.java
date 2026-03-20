package com.consumer.service;

import com.consumer.dto.TopSalesPerCityDTO;
import com.consumer.dto.TopSalesmanCountryDTO;
import com.consumer.entity.ResultEntity;
import com.consumer.repository.ResultRepository;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ResultService {

    private final ResultRepository repository;

    public ResultService(ResultRepository repository) {
        this.repository = repository;
    }

    public ResultEntity save(ResultEntity entity) {
        return repository.save(entity);
    }

    public List<ResultEntity> findAll() {
        return repository.findAll();
    }

    public List<TopSalesmanCountryDTO> getTopSalesmanCountry() {
        return repository.findTopSalesmanCountry();
    }

    public List<TopSalesPerCityDTO> getTopSalesPerCity() {
        return repository.findTopSalesPerCity();
    }
}
