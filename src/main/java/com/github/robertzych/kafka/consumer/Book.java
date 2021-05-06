package com.github.robertzych.kafka.consumer;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Book {
    @JsonProperty
    public int id;

    @JsonProperty
    public String title;

    public Book() {}

    public Book(int id, String title) {
        this.id = id;
        this.title = title;
    }
}
