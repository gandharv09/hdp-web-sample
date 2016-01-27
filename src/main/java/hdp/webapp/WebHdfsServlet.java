package hdp.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Created by hkropp on 19/01/16.
 */
public class WebHdfsServlet extends HttpServlet implements Servlet {

    public WebHdfsServlet(){};

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

        String path = request.getParameter("path");
        if (path == null) path = "/";

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","webhdfs://one.hdp:50070");

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fsStatus = fs.listStatus(new Path(path));

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

