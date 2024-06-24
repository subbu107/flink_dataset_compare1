package com.informatica.datavalidation;

public enum ComparisonSide {
    LEFT,
    RIGHT;

    public boolean isLeft() {
        return ComparisonSide.LEFT.equals(this);
    }
}
