package org.dataguru.kpi;

import java.io.Serializable;

import org.zj.util.StringUtil;

/*
 * 120.197.87.216 - - [04/Jan/2012:00:00:02 +0800] "GET /home.php?mod=space&uid=563413&mobile=yes HTTP/1.1" 200 3388 "-" "-"
 * 123.126.50.73 - - [04/Jan/2012:00:00:02 +0800] "GET /thread-679411-1-1.html HTTP/1.1" 200 5251 "-" "Sogou web spider/4.0(+http://www.sogou.com/docs/help/webmasters.htm#07)"
 * 203.208.60.187 - - [04/Jan/2012:00:00:02 +0800] "GET /archiver/tid-3003.html HTTP/1.1" 200 2056 "-" "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"
 * 114.112.141.6 - - [04/Jan/2012:00:00:02 +0800] "GET /ctp080113.php?action=getgold HTTP/1.1" 200 13886 "-" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"
 * 114.112.141.6 - - [04/Jan/2012:00:00:02 +0800] "GET /ctp080113.php?action=getmedal HTTP/1.1" 200 13882 "-" "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; InfoPath.3; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)"
 * 110.6.179.88 - - [04/Jan/2012:00:00:02 +0800] "GET /forum.php?mod=attachment&aid=NTczNzU3fDFjNDdjZTgzfDEzMjI4NzgwMDV8MTMzOTc4MDB8MTEwMTcxMA%3D%3D&mobile=no HTTP/1.1" 200 172 "http://www.itpub.net/forum.php?mod=attachment&aid=NTczNzU3fDFjNDdjZTgzfDEzMjI4NzgwMDV8MTMzOTc4MDB8MTEwMTcxMA%3D%3D&mobile=yes" "Mozilla/5.0 (Linux; U; Android 2.2; zh-cn; ZTE-U V880 Build/FRF91) UC AppleWebKit/530+ (KHTML, like Gecko) Mobile Safari/530"
 *
 */
public class LogLine implements Serializable {
	
	private static final long serialVersionUID = 1L;
    private String ip;
    private String url;
    private String status;
    private int pageSize;
    private String httpReferer;

    // 111.13.8.250 - - [05/Jan/2012:00:00:04 +0800] "GET /thread-476025-1-1.html HTTP/1.0" 200 99628 "-" "Mozilla/4.0"
    public static LogLine parse(String line) {
            if (line == null || line.length() == 0) {
                    return null;
            }
            int index = 0;
            // ip
            int endPos = line.indexOf("-");
            String ip = substring(line, index, endPos);
            index = endPos;
            // url
            index = line.indexOf(" /", index);
            endPos = line.indexOf("HTTP/", index);
            String url = substring(line, index, endPos);
            if (url!=null&&url.contains("?")) {
                    url = url.substring(0, url.indexOf("?"));
            }
            index = endPos;
            // status
            index = line.indexOf(" ", index);
            endPos = line.indexOf(" ", index + 1);
            String status = substring(line, index, endPos);
            index = endPos;
            // pageSize
            endPos = line.indexOf(" ", index + 1);
            String pageSize = substring(line, index, endPos);
            index = endPos;
            // httpReferer
            endPos = line.indexOf(" ", index + 1);
            String httpReferer = substring(line, index, endPos);
            index = endPos;

            if (StringUtil.empty(ip, url, status, pageSize, httpReferer)) {
                    return null;
            }

            LogLine ll = new LogLine();
            ll.setIp(ip);
            ll.setUrl(url);
            ll.setStatus(status);
            ll.setPageSize(Integer.parseInt(pageSize));
            ll.setHttpReferer(httpReferer);

            return ll;
    }

    private static String substring(String line, int index, int endPos) {
            if (index == -1) {
                    return null;
            }
            if (endPos == -1) {
                    return null;
            }
            String phase = null;
            try {
                    phase = line.substring(index, endPos).trim();
            } catch (Exception e) {
                    e.printStackTrace();
            }
            return phase;
    }

    /**
     * @return the ip
     */
    public String getIp() {
            return ip;
    }

    /**
     * @param ip
     *            the ip to set
     */
    public void setIp(String ip) {
            this.ip = ip;
    }

    /**
     * @return the url
     */
    public String getUrl() {
            return url;
    }

    /**
     * @param url
     *            the url to set
     */
    public void setUrl(String url) {
            this.url = url;
    }

    /**
     * @return the status
     */
    public String getStatus() {
            return status;
    }

    /**
     * @param status
     *            the status to set
     */
    public void setStatus(String status) {
            this.status = status;
    }

    /**
     * @return the pageSize
     */
    public int getPageSize() {
            return pageSize;
    }

    /**
     * @param pageSize
     *            the pageSize to set
     */
    public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
    }

    /**
     * @return the httpReferer
     */
    public String getHttpReferer() {
            return httpReferer;
    }

    /**
     * @param httpReferer
     *            the httpReferer to set
     */
    public void setHttpReferer(String httpReferer) {
            this.httpReferer = httpReferer;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
            return "NginxLine [httpReferer=" + httpReferer + ", ip=" + ip + ", pageSize=" + pageSize + ", status=" + status
                            + ", url=" + url + "]";
    }

	public static void main(String args[]) {
		//String line = "222.68.172.190 - - [18/Sep/2013:06:49:57 +0000] \"GET /images/my.jpg HTTP/1.1\" 200 19939 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/29.0.1547.66 Safari/537.36\"";
		String line = "120.197.87.216 - - [04/Jan/2012:00:00:02 +0800] \"GET /home.php?mod=space&uid=563413&mobile=yes HTTP/1.1\" 200 3388 \"-\" \"-\"";
		System.out.println(parse(line));
	}
}
