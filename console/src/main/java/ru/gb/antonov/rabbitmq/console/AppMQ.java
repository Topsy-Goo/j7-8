package ru.gb.antonov.rabbitmq.console;

import ru.gb.antonov.rabbitmq.console.producers.ItBlogger;

import java.util.Scanner;

public class AppMQ
{
    public static final String HOST_NAME = "localhost";
    public static final String EXCHANGER_NAME = "newsExchanger";


    public static void main (String[] args)
    {
        lnprint ("AppMQ: старт.");
        Scanner scanner = new Scanner (System.in);
        String text;
        ItBlogger itBlogger = new ItBlogger();

        while (scanner.hasNext ())
        {
            text = scanner.nextLine().trim();
            if (!text.isEmpty())
            {
                if (text.equalsIgnoreCase ("q"))
                {
                    itBlogger.stop();
                    break;
                }
                else itBlogger.publishNews (text);
            }
        }
        lnprint ("AppMQ: стоп.");
        scanner.close();
    }

    public static void lnprint (String s) { System.out.print ("\n"+ s); }

    public static String validString (String text)
    {
        if (text == null)   return null;
        text = text.trim();
        if (text.isEmpty ())    return null;
        return text;
    }
}
