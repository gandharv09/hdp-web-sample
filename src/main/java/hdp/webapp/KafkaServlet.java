package hdp.webapp;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import javax.servlet.Servlet;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;


/**
 * Created by hkropp on 21/02/16.
 */
public class KafkaServlet extends HttpServlet implements Servlet {

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        String topic = request.getParameter("topic");
        String msg = request.getParameter("msg");

        Properties props = new Properties();
        props.put("metadata.broker.list", "one.hdp:6667");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("security.protocol", "PLAINTEXTSASL");

        ProducerConfig config = new ProducerConfig(props);
        Producer producer = new Producer<String, String>(config);

        producer.send(new KeyedMessage<String, String>(topic, msg));

        PrintWriter out = response.getWriter();
        out.println("<html>");
        out.println("<head><title>Write to topic: "+ topic +"</title></head>");
        out.println("<body><h1>/"+ msg +"</h1>");
        out.println("</html>");
        out.close();

    }

}
