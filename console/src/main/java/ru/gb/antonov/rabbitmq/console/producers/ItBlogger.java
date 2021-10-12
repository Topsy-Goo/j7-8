package ru.gb.antonov.rabbitmq.console.producers;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;

import static ru.gb.antonov.rabbitmq.console.AppMQ.*;

public class ItBlogger
{
    private final ConnectionFactory connectionFactory;
    private static final BuiltinExchangeType builtinExchangeTypeTOPIC = BuiltinExchangeType.TOPIC;


    public ItBlogger()
    {
        connectionFactory = new ConnectionFactory ();
        connectionFactory.setHost (HOST_NAME);

        try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel())
        {
            channel.exchangeDeclare (EXCHANGER_NAME, builtinExchangeTypeTOPIC);
            System.out.printf ("\nItBlogger: создан: %s, %s\n", EXCHANGER_NAME, builtinExchangeTypeTOPIC.getType ());
        }
        catch (Exception e)
        {
            System.err.println ("\nНе удалось создать экземпляр ItBlogger.");
            e.printStackTrace();
        }
    }

    public void stop ()
    {
        try (Connection connection = connectionFactory.newConnection ();
             Channel channel = connection.createChannel ())
        {
            channel.exchangeDelete (EXCHANGER_NAME);
            System.out.printf ("\nItBlogger: удалён: %s, %s.", EXCHANGER_NAME, builtinExchangeTypeTOPIC.getType ());
        }
        catch (Exception e)
        {
            System.err.println ("\nНе удалось удалить экземпляр ItBlogger.");
            e.printStackTrace();
        }
        finally
        {   lnprint ("ItBlogger: стоп.");
        }
    }

    public void publishNews (String text)
    {
        String[] news = text.split (" ", 2);
        if (news.length > 1)
        {
            String topic = news[0];
            String body = validString (news[1]);
            if (body == null)
                return;

            try (Connection connection = connectionFactory.newConnection();
             Channel channel = connection.createChannel())
            {
                channel.basicPublish (EXCHANGER_NAME, topic, null, body.getBytes (StandardCharsets.UTF_8));
            }
            catch (Exception e){e.printStackTrace();}
            System.out.printf ("\nItBlogger: опубликована новость:\tтема: «%s», текст: «%s».\n", topic, body);
        }
    }
}
