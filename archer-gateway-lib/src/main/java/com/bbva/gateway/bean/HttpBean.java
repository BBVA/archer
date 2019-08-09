package com.bbva.gateway.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class HttpBean {

    private int code;
    private String body;
    private Map<String, List<String>> headers;

}
