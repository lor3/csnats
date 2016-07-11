// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using System.Threading;
using System.Diagnostics;
using System.Reflection;
using Xunit;

namespace NATSUnitTests
{
    class NATSServer : IDisposable
    {
        // Enable this for additional server debugging info.
        bool debug = false;
        Process p;

        public NATSServer()
        {
            ProcessStartInfo psInfo = createProcessStartInfo();
            this.p = Process.Start(psInfo);
            Thread.Sleep(500);
        }

        private void addArgument(ProcessStartInfo psInfo, string arg)
        {
            if (psInfo.Arguments == null)
            {
                psInfo.Arguments = arg;
            }
            else
            {
                string args = psInfo.Arguments;
                args += arg;
                psInfo.Arguments = args;
            }
        }

        public NATSServer(int port)
        {
            ProcessStartInfo psInfo = createProcessStartInfo();

            addArgument(psInfo, "-p " + port);

            this.p = Process.Start(psInfo);
        }

        public NATSServer( string args)
        {
            ProcessStartInfo psInfo = this.createProcessStartInfo();
            addArgument(psInfo, args);
            p = Process.Start(psInfo);
        }

        private ProcessStartInfo createProcessStartInfo()
        {
            string gnatsd = "gnatsd.exe";
            ProcessStartInfo psInfo = new ProcessStartInfo(gnatsd);

            if (debug)
            {
                psInfo.Arguments = " -DV ";
            }
            else
            {
               // psInfo.WindowStyle = ProcessWindowStyle.Hidden;
            }

            psInfo.WorkingDirectory =
                UnitTestUtilities.GetConfigDir();

            return psInfo;
        }

        public void Shutdown()
        {
            if (p == null)
                return;

            try
            {
                p.Kill();
            }
            catch (Exception) { }

            p = null;
        }

        void IDisposable.Dispose()
        {
            Shutdown();
        }
    }

    class ConditionalObj
    {
        Object objLock = new Object();
        bool completed = false;

        internal void wait(int timeout)
        {
            lock (objLock)
            {
                if (completed)
                    return;

                Assert.True(Monitor.Wait(objLock, timeout));
            }
        }

        internal void reset()
        {
            lock (objLock)
            {
                completed = false;
            }
        }

        internal void notify()
        {
            lock (objLock)
            {
                completed = true;
                Monitor.Pulse(objLock);
            }
        }
    }

    class UnitTestUtilities
    {
        Object mu = new Object();
        static NATSServer defaultServer = null;
        Process authServerProcess = null;

        static internal string GetConfigDir()
        {
            var baseDir = System.IO.Directory.GetCurrentDirectory();

			// hacky fix for command line / vs test runners
	        return System.IO.Directory.Exists(baseDir + "\\NATSUnitTests\\config")
		        ? baseDir + "\\NATSUnitTests\\config"
		        : baseDir + "\\config";
        }

        public void StartDefaultServer()
        {
            lock (mu)
            {
                if (defaultServer == null)
                {
                    defaultServer = new NATSServer();
                }
            }
        }

        public void StopDefaultServer()
        {
            lock (mu)
            {
                try
                {
                    defaultServer.Shutdown();
                }
                catch (Exception) { }

                defaultServer = null;
            }
        }

        public void bounceDefaultServer(int delayMillis)
        {
            StopDefaultServer();
            Thread.Sleep(delayMillis);
            StartDefaultServer();
        }

        public void startAuthServer()
        {
            authServerProcess = Process.Start("gnatsd -config auth.conf");
        }

        internal static void testExpectedException(Action call, Type exType)
        {
            try {
               call.Invoke();
            }
            catch (Exception e)
            {
                System.Console.WriteLine(e);
                Assert.True(e.GetType().GetTypeInfo().IsAssignableFrom(exType));
                return;
            }

            throw new Exception("No exception thrown!");
        }

        internal NATSServer CreateServerOnPort(int p)
        {
            return new NATSServer(p);
        }

        internal NATSServer CreateServerWithConfig(string configFile)
        {
            return new NATSServer(" -config " + configFile);
        }

        internal NATSServer CreateServerWithArgs(string args)
        {
            return new NATSServer(" " + args);
        }

        internal static String GetFullCertificatePath(string certificateName)
        {
            return GetConfigDir() + "\\certs\\" + certificateName;
        }

        internal static void CleanupExistingServers()
        {
            try
            {
                Process[] procs = Process.GetProcessesByName("gnatsd");

                foreach (Process proc in procs)
                {
                    proc.Kill();
                }
            }
            catch (Exception) { } // ignore
        }
    }
}
