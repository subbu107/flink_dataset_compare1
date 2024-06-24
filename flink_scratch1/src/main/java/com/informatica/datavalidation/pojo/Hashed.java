package com.informatica.datavalidation.pojo;

public class Hashed {
    private String leftKey;
    private String leftKeyHash;
    private String leftValueHash;

    private String rightKey;
    private String rightKeyHash;
    private String rightValueHash;

    public Hashed(String leftKey, String leftKeyHash, String leftValueHash, String rightKey, String rightKeyHash, String rightValueHash) {
        this.leftKey = leftKey;
        this.leftKeyHash = leftKeyHash;
        this.leftValueHash = leftValueHash;
        this.rightKey = rightKey;
        this.rightKeyHash = rightKeyHash;
        this.rightValueHash = rightValueHash;
    }

    public String getLeftKey() {
        return leftKey;
    }

    public void setLeftKey(String leftKey) {
        this.leftKey = leftKey;
    }

    public String getLeftKeyHash() {
        return leftKeyHash;
    }

    public void setLeftKeyHash(String leftKeyHash) {
        this.leftKeyHash = leftKeyHash;
    }

    public String getLeftValueHash() {
        return leftValueHash;
    }

    public void setLeftValueHash(String leftValueHash) {
        this.leftValueHash = leftValueHash;
    }

    public String getRightKey() {
        return rightKey;
    }

    public void setRightKey(String rightKey) {
        this.rightKey = rightKey;
    }

    public String getRightKeyHash() {
        return rightKeyHash;
    }

    public void setRightKeyHash(String rightKeyHash) {
        this.rightKeyHash = rightKeyHash;
    }

    public String getRightValueHash() {
        return rightValueHash;
    }

    public void setRightValueHash(String rightValueHash) {
        this.rightValueHash = rightValueHash;
    }
}
