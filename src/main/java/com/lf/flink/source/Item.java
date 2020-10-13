package com.lf.flink.source;

/**
 * @Classname Item
 * @Date 2020/9/25 下午7:05
 * @Created by fei.liu
 */
public class Item {
    private String name;
    private Integer id;

    public Item() {
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "Item{" +
                "name='" + name + '\'' +
                ", id=" + id +
                '}';
    }
}
