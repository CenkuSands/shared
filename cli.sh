oc get secrets | grep quay-registry-quay-config | awk '{print $1}' | xargs -I {} oc get secrets {}  -o jsonpath='{.data.config\.yaml}' | base64 -d | grep LDAP_ADMIN_PASSWD
