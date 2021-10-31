for cccc in AMMC BABJ EDZW OKBK OTBD OOMS RPMM VBRR VDPP VGDC VHHH VLIV VNKT VNNN VRMM VTBB; do
  aws iam create-user --user-name ${cccc}
  cccc_key_secret=`aws iam create-access-key --user-name ${cccc} | grep -E '("UserName":|"AccessKeyId":|"SecretAccessKey":)' | sed -e 's|^.*: "||g' -e 's|", *$||g' | tr '\n' ',' | sed -e 's|,$||g'`
  aws iam add-user-to-group --user-name ${cccc} --group-name publish
  cccc=`echo ${cccc_key_secret} | cut -d, -f1`
  key=`echo ${cccc_key_secret} | cut -d, -f2`
  secret=`echo ${cccc_key_secret} | cut -d, -f3`
  echo "" >> rclone.cof
  echo "[${cccc}_jma_aws]" >> rclone.cof
  echo "type = s3" >> rclone.cof
  echo "env_auth = false" >> rclone.cof
  echo "access_key_id = ${key}" >> rclone.cof
  echo "secret_access_key = ${secret}" >> rclone.cof
  echo "region = ap-northeast-1" >> rclone.cof
  echo "endpoint = https://s3.ap-northeast-1.amazonaws.com" >> rclone.cof
  echo "no_check_bucket = true" >> rclone.cof
done
