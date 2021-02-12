package com.example.kafkastreamsgroupbywindowed;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.With;

@Getter
@With
@NoArgsConstructor
@AllArgsConstructor
class AggregatedCount {
    private long nb;
    private long sum;
}
