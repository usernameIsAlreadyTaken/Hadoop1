package org.zj.util;

public class StringUtil {

	public static Boolean empty(String ip, String url, String status,
			String pageSize, String httpReferer) {
		if (0 == ip.length() || null == ip) {
			return true;
		}
		if (0 == url.length() || null == url) {
			return true;
		}
		if (0 == status.length() || null == status) {
			return true;
		}
		if (0 == pageSize.length() || null == pageSize) {
			return true;
		}
		if (0 == httpReferer.length() || null == httpReferer) {
			return true;
		}
		return false;
	}
}
