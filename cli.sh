oc get secrets | grep quay-registry-quay-config | awk '{print $1}' | xargs -I {} oc get secrets {}  -o jsonpath='{.data.config\.yaml}' | base64 -d | grep LDAP_ADMIN_PASSWD


oc get secrets -o name | grep quay-registry-quay-config | xargs -I {} oc get {} -o jsonpath='{.metadata.name}{"\t"}{.data.config\.yaml}{"\n"}' | while IFS=$'\t' read -r name b64; do [ -n "$b64" ] && decoded=$(echo "$b64" | base64 -d 2>/dev/null) && echo -e "Secret: $name\n$(echo "$decoded" | grep LDAP_ADMIN_PASSWD)\n----------------------------------------" || echo "Secret: $name\nError: Failed to decode config.yaml\n----------------------------------------"; done
