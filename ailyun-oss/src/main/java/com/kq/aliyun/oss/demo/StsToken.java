package com.kq.aliyun.oss.demo;

/**
 * @author kq
 * @date 2022-07-23 16:05
 * @since 2020-0630
 */
public class StsToken {

    private String accessKeyId;
    private String accessKeySecret;
    private String stsToken;

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessKeySecret() {
        return accessKeySecret;
    }

    public void setAccessKeySecret(String accessKeySecret) {
        this.accessKeySecret = accessKeySecret;
    }

    public String getStsToken() {
        return stsToken;
    }

    public void setStsToken(String stsToken) {
        this.stsToken = stsToken;
    }

    @Override
    public String toString() {
        return "StsToken{" +
                "accessKeyId='" + accessKeyId + '\'' +
                ", accessKeySecret='" + accessKeySecret + '\'' +
                ", stsToken='" + stsToken + '\'' +
                '}';
    }
}