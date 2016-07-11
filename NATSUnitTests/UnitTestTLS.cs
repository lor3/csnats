﻿// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using NATS.Client;
using System.Reflection;
using System.Linq;
using System.Threading;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestTLS
    {
        int hitDisconnect;

        UnitTestUtilities util = new UnitTestUtilities();

        private void connectAndFail(String url)
        {
            try
            {
                System.Console.WriteLine("Trying: " + url);

                hitDisconnect = 0;
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Url = url;
                opts.DisconnectedEventHandler += handleDisconnect;
                IConnection c = new ConnectionFactory().CreateConnection(url);

                throw new Exception("Expected a failure; did not receive one");
                
                c.Close();
            }
            catch (Exception e)
            {
                if (e.Message.Contains("Authorization"))
                {
                    System.Console.WriteLine("Success with expected failure: " + e.Message);
                }
                else
                {
                    throw new Exception("Unexpected exception thrown: " + e);
                }
            }
            finally
            {
                if (hitDisconnect > 0)
                    throw new Exception("The disconnect event handler was incorrectly invoked.");
            }
        }

        private void handleDisconnect(object sender, ConnEventArgs e)
        {
            hitDisconnect++;
        }


        // A hack to avoid issues with our test self signed cert.
        // We don't want to require the runner of the test to install the 
        // self signed CA, so we just manually compare the server cert
        // with the what the gnatsd server should return to the client
        // in our test.
        //
        // Getting here means SSL is working in the client.
        //
        private bool verifyServerCert(object sender,
            X509Certificate certificate, X509Chain chain,
            SslPolicyErrors sslPolicyErrors)
        {
            if (sslPolicyErrors == SslPolicyErrors.None)
                return true;

            X509Certificate serverCert = new X509Certificate(
                    UnitTestUtilities.GetFullCertificatePath(
                    "server-cert.pem"));

            // UNSAFE hack for testing purposes.
            if (serverCert.GetPublicKey().SequenceEqual(certificate.GetPublicKey()))
                return true;

            return false;
        }

        [Fact]
        public void TestTlsSuccessWithCert()
        {
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222_verify.conf"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.Url = "nats://localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // .NET requires the private key and cert in the 
                //  same file. 'client.pfx' is generated from:
                //
                // openssl pkcs12 -export -out client.pfx 
                //    -inkey client-key.pem -in client-cert.pem
                X509Certificate2 cert = new X509Certificate2(
                    UnitTestUtilities.GetFullCertificatePath(
                        "client.pfx"), "password");

                opts.AddCertificate(cert);

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Msg m = s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");
                    }
                }
            }
        }

        [Fact]
        public void TestTlsFailWithCert()
        {
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222_verify.conf"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.Url = "nats://localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath(
                        "client-cert.pem"));

                try
                {
                    new ConnectionFactory().CreateConnection(opts);
                }
                catch (NATSException nae)     
                {
                    System.Console.WriteLine("Caught expected exception: " + nae.Message);
                    return;
                }

                throw new Exception("Did not receive exception.");
            }
        }

        [Fact]
        public void TestTlsFailWithBadAuth()
        {
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222_user.conf"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.Url = "nats://username:BADDPASSOWRD@localhost:1222";
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                // this will fail, because it's not complete - missing the private
                // key.
                opts.AddCertificate(UnitTestUtilities.GetFullCertificatePath(
                        "client-cert.pem"));

                try
                {
                    new ConnectionFactory().CreateConnection(opts);
                }
                catch (NATSException nae)
                {
                    System.Console.WriteLine("Caught expected exception: " + nae.Message);
                    System.Console.WriteLine("Exception output:" + nae);
                    return;
                }

                throw new Exception("Did not receive exception.");
            }
        }

        [Fact]
        public void TestTlsSuccessSecureConnect()
        {
            using (NATSServer srv = util.CreateServerWithConfig("tls_1222.conf"))
            {
                // we can't call create secure connection w/ the certs setup as they are
                // so we'll override the 
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
                opts.Url = "nats://localhost:1222";

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Msg m = s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");
                    }
                }
            }
        }

        [Fact]
        public void TestTlsReconnect()
        {
            ConditionalObj reconnectedObj = new ConditionalObj();

            using (NATSServer srv = util.CreateServerWithConfig("tls_1222.conf"),
                   srv2 = util.CreateServerWithConfig("tls_1224.conf"))
            {
                System.Threading.Thread.Sleep(1000);

                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;
                opts.Servers = new string[]{ "nats://localhost:1222" , "nats://localhost:1224" };
                opts.ReconnectedEventHandler += (sender, obj) =>
                {
                    Console.WriteLine("Reconnected.");
                    reconnectedObj.notify();
                };

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    // send a message to be sure.
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");

                        // shutdown the server
                        srv.Shutdown();

                        // wait for reconnect
                        reconnectedObj.wait(30000);

                        c.Publish("foo", null);
                        c.Flush();
                        s.NextMessage();
                        System.Console.WriteLine("Received msg over TLS conn.");
                    }
                }
            }
        }

        // Tests if reconnection can still occur after a user authorization failure
        // under TLS.
        [Fact]
        public void TestTlsReconnectAuthTimeout()
        {
            ConditionalObj obj = new ConditionalObj();

            using (NATSServer s1 = util.CreateServerWithConfig("auth_tls_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_tls_1223_timeout.conf"),
                              s3 = util.CreateServerWithConfig("auth_tls_1224.conf"))
            {

                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1223", 
                    "nats://username:password@localhost:1224" };

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Reconnected");
                    obj.notify();
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);
                s1.Shutdown();

                // This should fail over to S2 where an authorization timeout occurs
                // then successfully reconnect to S3.

                obj.wait(20000);
            }
        }

        [Fact]
        public void TestTlsReconnectAuthTimeoutLateClose()
        {
            ConditionalObj obj = new ConditionalObj();

            using (NATSServer s1 = util.CreateServerWithConfig("auth_tls_1222.conf"),
                              s2 = util.CreateServerWithConfig("auth_tls_1224.conf"))
            {

                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Secure = true;
                opts.NoRandomize = true;
                opts.TLSRemoteCertificationValidationCallback = verifyServerCert;

                opts.Servers = new string[]{
                    "nats://username:password@localhost:1222",
                    "nats://username:password@localhost:1224" };

                opts.ReconnectedEventHandler += (sender, args) =>
                {
                    obj.notify();
                };

                IConnection c = new ConnectionFactory().CreateConnection(opts);

                // inject an authorization timeout, as if it were processed by an incoming server message.
                // this is done at the parser level so that parsing is also tested,
                // therefore it needs reflection since Parser is an internal type.
                Type parserType = typeof(Connection).GetTypeInfo().Assembly.GetType("NATS.Client.Parser");
                Assert.True(parserType != null, "Failed to find NATS.Client.Parser");
                BindingFlags flags = BindingFlags.NonPublic | BindingFlags.Instance;
                object parser = Activator.CreateInstance(parserType, c);
                Assert.True(parser != null, "Failed to instanciate a NATS.Client.Parser");
                MethodInfo parseMethod = parserType.GetMethod("parse", flags);
                Assert.True(parseMethod != null, "Failed to find method parse in NATS.Client.Parser");

                byte[] bytes = "-ERR 'Authorization Timeout'\r\n".ToCharArray().Select(ch => (byte)ch).ToArray();
                parseMethod.Invoke(parser, new object[] { bytes, bytes.Length });

                // sleep to allow the client to process the error, then shutdown the server.
                Thread.Sleep(250);
                s1.Shutdown();

                // Wait for a reconnect.
                obj.wait(20000);
            }
        }
    }
}
