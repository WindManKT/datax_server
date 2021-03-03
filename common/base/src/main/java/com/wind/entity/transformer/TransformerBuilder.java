package com.wind.entity.transformer;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class TransformerBuilder {
    private List<UDF> udfList = new ArrayList<>();
}
