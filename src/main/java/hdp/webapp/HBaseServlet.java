package hdp.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Created by hkropp on 20/01/16.
 */
public class HBaseServlet extends HttpServlet implements Servlet {

    public HBaseServlet() {}

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "one.hdp");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        config.set("hbase.rootdir", "/apps/hbase/data");


        config.set("zookeeper.znode.parent", "/hbase-unsecure");

        config.set("hadoop.security.authentication", "kerberos");
        config.set("hbase.security.authentication", "kerberos");
        config.set("hbase.master.kerberos.principal", "hbase/_HOST@MYCORP.NET");
        config.set("hbase.regionserver.kerberos.principal", "hbase/_HOST@MYCORP.NET");
        config.set("hbase.client.retries.number", "10");

        config.set("hbase.rootdir", "/apps/hbase/data");
        config.set("zookeeper.znode.parent", "/hbase-secure");

        UserGroupInformation.setConfiguration(config);
        UserGroupInformation.loginUserFromKeytab("tomcat@MYCORP.NET", "/etc/tomcat/tomcat.keytab");

        Connection conn = ConnectionFactory.createConnection(config);
        Admin adm = conn.getAdmin();

        TableName[] tblNames = adm.listTableNames();

        //String[] tblNames = new String[]{"s","s"};

        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head><title>hbase list</title></head>");
        out.println("<body><h1>HBase Tables:</h1><ul>");

        for(int i = 0; i < tblNames.length; i++){
            out.println("<li>" + tblNames[i].toString() + "</li>");
        }

        out.println("</ul></body>");
        out.println("</html>");
        out.close();

    }

}