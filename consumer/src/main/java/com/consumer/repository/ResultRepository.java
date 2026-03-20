package com.consumer.repository;

import com.consumer.dto.TopSalesPerCityDTO;
import com.consumer.dto.TopSalesmanCountryDTO;
import com.consumer.entity.ResultEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface ResultRepository extends JpaRepository<ResultEntity, Long> {
    @Query("""
        SELECT new com.consumer.dto.TopSalesmanCountryDTO(r.salesman, SUM(r.totalAmount))
        FROM ResultEntity r
        GROUP BY r.salesman
        ORDER BY SUM(r.totalAmount) DESC
    """)
    List<TopSalesmanCountryDTO> findTopSalesmanCountry();

    @Query("""
        SELECT new com.consumer.dto.TopSalesPerCityDTO(r.city, SUM(r.totalAmount))
        FROM ResultEntity r
        GROUP BY r.city
        ORDER BY SUM(r.totalAmount) DESC
    """)
    List<TopSalesPerCityDTO> findTopSalesPerCity();
}
