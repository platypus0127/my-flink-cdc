package com.hsb.reader;

import java.io.Serializable;

public class MMetaColumn implements Serializable {
    private String name;            // 字段名
    private String type;            // 字段类型
    private String iskey;           // 字段是否有约束
    private String comment;         // 字段注释

    public MMetaColumn() {}

    public MMetaColumn(String n, String t) {
        name = n;
        type = t;
    }

    public String getIskey() {
        return iskey;
    }

    public void setIskey(String iskey) {
        this.iskey = iskey;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }
}
