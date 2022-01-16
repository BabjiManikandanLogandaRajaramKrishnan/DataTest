package com.s4m.datatest.entity;

public class Point {

    private double gpsLatitude;
    private double gpsLongitude;
    private long storeId;
    private long distanceFromStore;

    public double getGpsLatitude() {
        return gpsLatitude;
    }

    public void setGpsLatitude(double gpsLatitude) {
        this.gpsLatitude = gpsLatitude;
    }

    public double getGpsLongitude() {
        return gpsLongitude;
    }

    public void setGpsLongitude(double gpsLongitude) {
        this.gpsLongitude = gpsLongitude;
    }

    public long getStoreId() {
        return storeId;
    }

    public void setStoreId(long storeId) {
        this.storeId = storeId;
    }

    public long getDistanceFromStore() {
        return distanceFromStore;
    }

    public void setDistanceFromStore(long distanceFromStore) {
        this.distanceFromStore = distanceFromStore;
    }
}
