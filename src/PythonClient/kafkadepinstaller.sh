mkdir kafkadownloads
cd kafkadownloads
curl http://ftp.us.debian.org/debian/pool/main/g/glibc/multiarch-support_2.19-18+deb8u10_amd64.deb -O
md5=$(md5sum multiarch-support_2.19-18+deb8u10_amd64.deb | cut -b 1-32)
if [[ "$md5" != 'c3146eaa5ba5f757d1088b5217c2a313' ]]; then
 echo "Invalid Md5Hash for multiarch-support: $md5" 1>&2
 exit 1
fi
dpkg -i multiarch-support_2.19-18+deb8u10_amd64.deb

curl http://security.debian.org/debian-security/pool/updates/main/o/openssl/libssl1.0.0_1.0.1t-1+deb8u12_amd64.deb -O
md5=$(md5sum libssl1.0.0_1.0.1t-1+deb8u12_amd64.deb | cut -b 1-32)
if [[ "$md5" != "02124c56a3fa64ab3f9a225f450dc0ac" ]]; then
 echo "Invalid Md5Hash for libssl: $md5" 1>&2
 exit 1
fi
dpkg -i libssl1.0.0_1.0.1t-1+deb8u12_amd64.deb
cd ..
rm -rdf kafkadownloads