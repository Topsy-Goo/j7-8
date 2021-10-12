package ru.gb.antonov.rabbitmq.console.consumers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static ru.gb.antonov.rabbitmq.console.AppMQ.*;

public class LanguageConsumer
{
    private ConnectionFactory connectionFactory;
    private Connection        connection;
    private Channel           channel;
    private final Set<String> topics;

    public static void main (String[] args)
    {
        lnprint ("LanguageConsumer: старт.\n");
        Scanner          scanner          = new Scanner (System.in);
        String           text;
        LanguageConsumer languageConsumer = new LanguageConsumer();

        while (scanner.hasNext ())
        {
            text = scanner.nextLine().trim();
            if (!text.isEmpty())
            {
                if (text.equalsIgnoreCase ("q"))
                {
                    languageConsumer.stop();
                    break;
                }
                else if (text.startsWith ("+"))
                {
                    languageConsumer.subscribe (text.substring (1));
                }
                else if (text.startsWith ("-"))
                {
                    languageConsumer.unsubscribe (text.substring (1));
                }
                else System.err.println ("Не могу обработать команду: «"+ text + "».");
            }
        }
        lnprint ("LanguageConsumer: стоп.");
        scanner.close();
    }

    public LanguageConsumer ()
    {
        topics = new HashSet<>();
        connectionFactory = new ConnectionFactory ();
        connectionFactory.setHost (HOST_NAME);
        try
        {
            connection = connectionFactory.newConnection ();
            channel = connection.createChannel();
        }
        catch (IOException | TimeoutException e){e.printStackTrace ();}
    }

    public void stop ()
    {
        for (String queue : topics)
            unsubscribe (queue);
        try
        {   channel.close ();
            connection.close();
        }
        catch (Exception e){e.printStackTrace();}
    }

    public void subscribe (String theme)
    {
        theme = validString (theme);
        if (theme == null)
            return;
        if (topics.contains (theme))
            System.out.printf ("\nLanguageConsumer: уже подписан на тему: «%s».\n", theme);
        else
        {   try
            {   if (channel.isOpen())
                {
                    String queueName = channel.queueDeclare (theme, false, false, false, null).getQueue();
                    channel.queueBind (queueName, EXCHANGER_NAME, theme);
                    topics.add (theme);

                    DeliverCallback deliverCallback = (consumerTag, delivery) ->
                    {
                        String message = new String (delivery.getBody(), StandardCharsets.UTF_8);
                        System.out.printf ("\nLanguageConsumer: получено сообщение: текст: «%s».\n", message);
                        channel.basicAck (delivery.getEnvelope().getDeliveryTag(), false);
                    };
                    channel.basicConsume (theme, false, deliverCallback, consumerTag -> {});
                    System.out.printf ("\nLanguageConsumer: подписался на тему:\t«%s»\n", theme);
                }
            }
            catch (IOException e){e.printStackTrace ();}
        }
    }

    public void unsubscribe (String text)
    {
        text = validString (text);
        if (text == null)
            return;
        if (topics.contains (text))
        {
            try
            {   if (channel.isOpen())
                {
                    channel.queueDelete (text);
                    topics.remove (text);
                    System.out.printf ("\nLanguageConsumer: отписался от темы:\t«%s»\n", text);
                }
            }
            catch (IOException e){e.printStackTrace ();}
        }
        else
            System.out.printf ("\nLanguageConsumer: не могу отписаться от темы: «%s», — не был подписан на неё.\n", text);
    }
}
