package com.kq.aliyun.oss.demo;

import com.aliyun.oss.ClientException;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.auth.sts.AssumeRoleRequest;
import com.aliyuncs.auth.sts.AssumeRoleResponse;
import com.aliyuncs.exceptions.ServerException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;

/**
 * @author kq
 * @date 2022-07-23 16:00
 * @since 2020-0630
 */
public class GetStsToken {

    public static void main(String[] args) {

        String endpoint = "endPoint";
        String accessKeyId = "accesskeyid";
        String accessKeySecret = "accesskeysecret";
        String roleArn = "rolearn";
        String roleSessionName = "rolesessionName";
        try {
            // 添加endpoint（直接使用STS endpoint，前两个参数留空，无需添加region ID）
            DefaultProfile.addEndpoint("", "", "Sts", endpoint);
            // 构造default profile（参数留空，无需添加region ID）
            IClientProfile profile = DefaultProfile.getProfile("", accessKeyId, accessKeySecret);
            // 用profile构造client
            DefaultAcsClient client = new DefaultAcsClient(profile);
            final AssumeRoleRequest request = new AssumeRoleRequest();
            request.setMethod(MethodType.POST);
            request.setRoleArn(roleArn);
            request.setRoleSessionName(roleSessionName);
            request.setDurationSeconds(1000L); // 设置凭证有效时间
            final AssumeRoleResponse response = client.getAcsResponse(request);
//            log.info("->Expiration is -> {}:", response.getCredentials().getExpiration() + ",Access Key Id:" + response.getCredentials().getAccessKeyId() + ",Access Key Secret:" + response.getCredentials().getAccessKeySecret() + ",Security Token:" + response.getCredentials().getSecurityToken() + ",RequestId:" + response.getRequestId());
            StsToken stsToken = new StsToken();
            stsToken.setAccessKeyId(response.getCredentials().getAccessKeyId());
            stsToken.setAccessKeySecret(response.getCredentials().getAccessKeySecret());
            stsToken.setStsToken(response.getCredentials().getSecurityToken());


            System.out.println("stsToken="+stsToken);

        } catch (ClientException e) {
            e.printStackTrace();
//            log.info("->Error code: is -> {}:", e.getErrorCode() + ",Error message:" + e.getErrorMessage() + ",RequestId:" + e.getRequestId());
        } catch (ServerException e) {
            e.printStackTrace();
        } catch (com.aliyuncs.exceptions.ClientException e) {
            e.printStackTrace();
        }

    }




}
