package hdp.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created by hkropp on 19/01/16.
 */
public class HdfsServlet extends HttpServlet implements Servlet {

    public HdfsServlet(){};

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String user = request.getParameter("user");

        String path = request.getParameter("path");
        if (path == null) path = "/";

        final Path p = new Path(path);

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://one.hdp:8020");

        conf.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("tomcat/one.hdp@MYCORP.NET", "/etc/tomcat/tomcat.keytab");

        FileSystem fs = null;
        FileStatus[] fsStatus = null;

        // NO PROXY USER METHOD
        if( user == null ) {
            fs = FileSystem.get(conf);
            fsStatus = fs.listStatus(p);
        } else { // WITH PROXY USER METHOD
            //hadoop.proxyuser.tomcat.hosts=*
            //hadoop.proxyuser.tomcat.groups=*
            UserGroupInformation proxyUser = UserGroupInformation.getCurrentUser();
            UserGroupInformation ugi = UserGroupInformation.createProxyUser(user, proxyUser);
            try {
                fsStatus = ugi.doAs(new PrivilegedExceptionAction<FileStatus[]>() {
                    public FileStatus[] run() throws IOException {
                        return FileSystem.get(conf).listStatus(p);
                    }
                });
            }
            catch (InterruptedException e) { e.printStackTrace(); }
        }

        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head><title>hdfs dfs -ls /</title></head>");
        out.println("<body><h1>/</h1><ul>");

        for(int i = 0; i < fsStatus.length; i++){
            out.println("<li>" + fsStatus[i].getPath().toString() + "</li>");
        }
        out.println("</ul></body>");
        out.println("</html>");
        out.close();
    }
}
