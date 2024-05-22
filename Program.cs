using Klasy;
using MassTransit;
using MassTransit.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Wydawca
{
    class Consumer : IConsumer<IAbonA>, IConsumer<IAbonB>
    {
        public Task Consume(ConsumeContext<IAbonA> context)
        {
            return Task.Run(() =>
            {
                var random = new Random();
                if (random.Next(0, 3) == -1)
                {
                    Console.WriteLine($"Wyjatek - {context.Message.Who}");
                    throw new Exception($"To nie jest rzeczywisty wyjatek");
                }
                Console.WriteLine($"Otrzymano odpowiedz od: {context.Message.Who}");
            });
        }
        public Task Consume(ConsumeContext<IAbonB> context)
        {
            return Task.Run(() =>
            {
                var random = new Random();
                if (random.Next(0, 3) == -1)
                {
                    Console.WriteLine($"Wyjatek - {context.Message.Who}");
                    throw new Exception($"To nie jest rzeczywisty wyjatek");
                }
                Console.WriteLine($"Otrzymano odpowiedz od: {context.Message.Who}");
            });
        }
    }

    public class ControllerHandler : IConsumer<IKontr>
    {
        public bool IsActive { get; private set; } = true;
        public Task Consume(ConsumeContext<IKontr> context)
        {
            this.IsActive = context.Message.Active;
            return Task.Run(() =>
                Console.WriteLine($"Generator zostal {(this.IsActive ? "aktywowany" : "dezaktywowany")}"));
        }
    }

    class Observer : IPublishObserver, IConsumeMessageObserver<IAbonA>, IConsumeMessageObserver<IAbonB>, IConsumeMessageObserver<IKontr>
    {
        public int publishCounter = 0;
        public int tryConsumeAbonACounter = 0;
        public int successConsumeAbonACounter = 0;
        public int tryConsumeAbonBCounter = 0;
        public int successConsumeAbonBCounter = 0;
        public int tryConsumeCtrlCounter = 0;
        public int successConsumeCtrlCounter = 0;

        public int publishA = 0;
        public int publishB = 0;
        public int publishCtrl = 0;

        public Dictionary<Type, int> opublikowany = new Dictionary<Type, int>();

        
        public Task PostPublish<T>(PublishContext<T> context) where T : class
        {
            Type t = context.Message.GetType();
            if (opublikowany.TryGetValue(t, out int count))
            {
                opublikowany[t] = count + 1;
            }
            else
            {
                opublikowany.Add(t, 1);
            }
            return Task.CompletedTask;
        }

        public Task PrePublish<T>(PublishContext<T> context) where T : class
        {
            return Task.Run(() => { });
        }

        public Task PublishFault<T>(PublishContext<T> context, Exception exception) where T : class
        {
            return Task.Run(() => { });
        }

        public Task PreConsume(ConsumeContext<IAbonA> context)
        {
            return Task.Run(() => { tryConsumeAbonACounter += 1; });
        }

        public Task PostConsume(ConsumeContext<IAbonA> context)
        {
            return Task.Run(() => { successConsumeAbonACounter += 1; });
        }

        public Task ConsumeFault(ConsumeContext<IAbonA> context, Exception exception)
        {
            return Task.Run(() => { });
        }

        public Task PreConsume(ConsumeContext<IAbonB> context)
        {
            return Task.Run(() => { tryConsumeAbonBCounter += 1; });
        }

        public Task PostConsume(ConsumeContext<IAbonB> context)
        {
            return Task.Run(() => { successConsumeAbonBCounter += 1; });
        }

        public Task ConsumeFault(ConsumeContext<IAbonB> context, Exception exception)
        {
            return Task.Run(() => { });
        }

        public Task PreConsume(ConsumeContext<IKontr> context)
        {
            return Task.Run(() => { tryConsumeCtrlCounter += 1; });
        }

        public Task PostConsume(ConsumeContext<IKontr> context)
        {
            return Task.Run(() => { successConsumeCtrlCounter += 1; });
        }

        public Task ConsumeFault(ConsumeContext<IKontr> context, Exception exception)
        {
            return Task.Run(() => { });
        }
    }
    
    internal class Program
    {
        static void Main(string[] args)
        {
            var controllerHandler = new ControllerHandler();
            var consumer = new Consumer();
            var observer = new Observer();
            bool active = true;

            var activeBus = Bus.Factory.CreateUsingRabbitMq(sbc =>
            {
                sbc.Host(new Uri("rabbitmq://localhost/"),
                h => { h.Username("guest"); h.Password("guest"); });
                sbc.UseEncryptedSerializer(
                   new AesCryptoStreamProvider(
                       new Supplier("18870118870118870118870118870118"),
                       "1887011887011887"));
                sbc.ReceiveEndpoint("ctrl", ep => {
                    ep.Instance(controllerHandler);
                });
            });

            activeBus.ConnectConsumeMessageObserver<IKontr>(observer);

            var publishBus = Bus.Factory.CreateUsingRabbitMq(sbc =>
            {
                sbc.Host(new Uri("rabbitmq://localhost/"),
                h => { h.Username("guest"); h.Password("guest"); });
                sbc.ReceiveEndpoint("Wydawca", ep => {
                    ep.Instance(consumer);
                    ep.UseRetry(r => { r.Immediate(5); });
                });
            });
            publishBus.ConnectConsumeMessageObserver<IAbonA>(observer);
            publishBus.ConnectConsumeMessageObserver<IAbonB>(observer);
            publishBus.ConnectPublishObserver(observer);

            activeBus.Start();
            publishBus.Start();
            Console.WriteLine("Rozpoczęto publikacje");

            int counter = 1;
            Task.Run(() => {
                while (active)
                {
                    while (controllerHandler.IsActive)
                    {
                        publishBus.Publish(new Wyd() { Number = counter });
                        Console.WriteLine($"Publ {counter++}");
                        Thread.Sleep(1000);
                    }
                }
            });

            ConsoleKey key;
            while ((key = Console.ReadKey().Key) != ConsoleKey.Escape)
            {
                if (key == ConsoleKey.S)
                {
                    Console.WriteLine($"\nStatystyki: ");
                    Console.WriteLine($"\tLiczba opublikowanych wiadmosci w zaleznosci od Typu:");
                    foreach (var typ in observer.opublikowany)
                    {
                        Console.WriteLine($"\t{typ.Key}: {typ.Value}");
                    }
                    Console.WriteLine($"\tProb obsluzenia od OdbiorcaA: {observer.tryConsumeAbonACounter}");
                    Console.WriteLine($"\tPomyslnie obsluzony OdbiorcaA: {observer.successConsumeAbonACounter}");
                    Console.WriteLine($"\tNiepomyslnie obsluzony OdbiorcaA: {observer.tryConsumeAbonACounter - observer.successConsumeAbonACounter}");
                    Console.WriteLine($"\tWyslane do OdbiorcaB: {observer.tryConsumeAbonBCounter}");
                    Console.WriteLine($"\tPomyslnie obsluzony OdbiorcaB: {observer.successConsumeAbonBCounter}");
                    Console.WriteLine($"\tNiepomyslnie obsluzony OdbiorcaB: {observer.tryConsumeAbonBCounter - observer.successConsumeAbonBCounter}");
                    Console.WriteLine($"\tWyslane do kontroler: {observer.tryConsumeCtrlCounter}");
                    Console.WriteLine($"\tPomyslnie obsluzony Kontroler: {observer.successConsumeCtrlCounter}");
                    Console.WriteLine($"\tNiepomyslnie obsluzony Kontroler: {observer.tryConsumeCtrlCounter - observer.successConsumeCtrlCounter}");
                }
            }
            activeBus.Stop();
            publishBus.Stop();
            Console.WriteLine("Zakończono publikacje");
        }
    }
}