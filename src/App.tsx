import React, { useState, useEffect } from 'react';
import { 
  Activity, 
  AlertTriangle, 
  BarChart3, 
  Database, 
  Globe, 
  Monitor, 
  Play, 
  Server, 
  Shield, 
  TrendingUp,
  Zap,
  CheckCircle,
  XCircle,
  Clock,
  Users,
  DollarSign,
  Cpu
} from 'lucide-react';

interface ServiceStatus {
  name: string;
  status: 'running' | 'stopped' | 'error';
  port?: number;
  description: string;
}

interface MetricCard {
  title: string;
  value: string;
  change: string;
  trend: 'up' | 'down' | 'stable';
  icon: React.ReactNode;
  color: string;
}

function App() {
  const [currentTime, setCurrentTime] = useState(new Date());
  const [activeTab, setActiveTab] = useState('overview');

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 1000);
    return () => clearInterval(timer);
  }, []);

  const services: ServiceStatus[] = [
    { name: 'Kafka', status: 'running', port: 9092, description: 'Message Streaming' },
    { name: 'Redis', status: 'running', port: 6379, description: 'Feature Cache' },
    { name: 'InfluxDB', status: 'running', port: 8086, description: 'Time Series DB' },
    { name: 'Elasticsearch', status: 'running', port: 9200, description: 'Search & Analytics' },
    { name: 'Grafana', status: 'running', port: 3000, description: 'Monitoring' },
    { name: 'Kibana', status: 'running', port: 5601, description: 'Log Analysis' }
  ];

  const metrics: MetricCard[] = [
    {
      title: 'Events/Second',
      value: '47,832',
      change: '+12.5%',
      trend: 'up',
      icon: <Zap className="w-6 h-6" />,
      color: 'text-blue-600'
    },
    {
      title: 'Risk Scores Processed',
      value: '2.1M',
      change: '+8.3%',
      trend: 'up',
      icon: <Shield className="w-6 h-6" />,
      color: 'text-green-600'
    },
    {
      title: 'High Risk Events',
      value: '1,247',
      change: '-2.1%',
      trend: 'down',
      icon: <AlertTriangle className="w-6 h-6" />,
      color: 'text-red-600'
    },
    {
      title: 'Active Users',
      value: '18,429',
      change: '+5.7%',
      trend: 'up',
      icon: <Users className="w-6 h-6" />,
      color: 'text-purple-600'
    }
  ];

  const getStatusIcon = (status: ServiceStatus['status']) => {
    switch (status) {
      case 'running':
        return <CheckCircle className="w-5 h-5 text-green-500" />;
      case 'error':
        return <XCircle className="w-5 h-5 text-red-500" />;
      default:
        return <Clock className="w-5 h-5 text-yellow-500" />;
    }
  };

  const getTrendIcon = (trend: 'up' | 'down' | 'stable') => {
    if (trend === 'up') return '↗️';
    if (trend === 'down') return '↘️';
    return '→';
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-900 via-slate-800 to-slate-900">
      {/* Header */}
      <header className="bg-slate-800/50 backdrop-blur-sm border-b border-slate-700">
        <div className="max-w-7xl mx-auto px-6 py-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-3">
                <div className="p-2 bg-gradient-to-r from-blue-500 to-purple-600 rounded-lg">
                  <Activity className="w-8 h-8 text-white" />
                </div>
                <div>
                  <h1 className="text-2xl font-bold text-white">Risk Scoring Platform</h1>
                  <p className="text-slate-400 text-sm">Real-Time Event Processing</p>
                </div>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              <div className="text-right">
                <div className="text-white font-medium">
                  {currentTime.toLocaleTimeString()}
                </div>
                <div className="text-slate-400 text-sm">
                  {currentTime.toLocaleDateString()}
                </div>
              </div>
              <div className="w-3 h-3 bg-green-400 rounded-full animate-pulse"></div>
            </div>
          </div>
        </div>
      </header>

      {/* Navigation */}
      <nav className="bg-slate-800/30 backdrop-blur-sm border-b border-slate-700">
        <div className="max-w-7xl mx-auto px-6">
          <div className="flex space-x-8">
            {[
              { id: 'overview', label: 'Overview', icon: <Monitor className="w-4 h-4" /> },
              { id: 'services', label: 'Services', icon: <Server className="w-4 h-4" /> },
              { id: 'analytics', label: 'Analytics', icon: <BarChart3 className="w-4 h-4" /> },
              { id: 'testing', label: 'Testing', icon: <Play className="w-4 h-4" /> }
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`flex items-center space-x-2 px-4 py-3 border-b-2 transition-colors ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-400'
                    : 'border-transparent text-slate-400 hover:text-white'
                }`}
              >
                {tab.icon}
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>
      </nav>

      {/* Main Content */}
      <main className="max-w-7xl mx-auto px-6 py-8">
        {activeTab === 'overview' && (
          <div className="space-y-8">
            {/* Metrics Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
              {metrics.map((metric, index) => (
                <div key={index} className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                  <div className="flex items-center justify-between mb-4">
                    <div className={metric.color}>
                      {metric.icon}
                    </div>
                    <span className="text-2xl">{getTrendIcon(metric.trend)}</span>
                  </div>
                  <div>
                    <h3 className="text-slate-400 text-sm font-medium">{metric.title}</h3>
                    <p className="text-2xl font-bold text-white mt-1">{metric.value}</p>
                    <p className={`text-sm mt-1 ${
                      metric.trend === 'up' ? 'text-green-400' : 
                      metric.trend === 'down' ? 'text-red-400' : 'text-slate-400'
                    }`}>
                      {metric.change} from last hour
                    </p>
                  </div>
                </div>
              ))}
            </div>

            {/* Quick Actions */}
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
              <h2 className="text-xl font-bold text-white mb-6 flex items-center">
                <Play className="w-5 h-5 mr-2" />
                Quick Actions
              </h2>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <button className="flex items-center space-x-3 p-4 bg-blue-600/20 hover:bg-blue-600/30 rounded-lg border border-blue-500/30 transition-colors group">
                  <Zap className="w-6 h-6 text-blue-400 group-hover:text-blue-300" />
                  <div className="text-left">
                    <div className="text-white font-medium">Start Producer</div>
                    <div className="text-slate-400 text-sm">Begin data generation</div>
                  </div>
                </button>
                <button className="flex items-center space-x-3 p-4 bg-green-600/20 hover:bg-green-600/30 rounded-lg border border-green-500/30 transition-colors group">
                  <BarChart3 className="w-6 h-6 text-green-400 group-hover:text-green-300" />
                  <div className="text-left">
                    <div className="text-white font-medium">View Metrics</div>
                    <div className="text-slate-400 text-sm">Open Grafana dashboard</div>
                  </div>
                </button>
                <button className="flex items-center space-x-3 p-4 bg-purple-600/20 hover:bg-purple-600/30 rounded-lg border border-purple-500/30 transition-colors group">
                  <Database className="w-6 h-6 text-purple-400 group-hover:text-purple-300" />
                  <div className="text-left">
                    <div className="text-white font-medium">Query Data</div>
                    <div className="text-slate-400 text-sm">Access Kibana interface</div>
                  </div>
                </button>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'services' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white flex items-center">
              <Server className="w-6 h-6 mr-3" />
              Infrastructure Services
            </h2>
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {services.map((service, index) => (
                <div key={index} className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                  <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center space-x-3">
                      {getStatusIcon(service.status)}
                      <h3 className="text-lg font-semibold text-white">{service.name}</h3>
                    </div>
                    {service.port && (
                      <span className="text-xs bg-slate-700 text-slate-300 px-2 py-1 rounded">
                        :{service.port}
                      </span>
                    )}
                  </div>
                  <p className="text-slate-400 text-sm mb-4">{service.description}</p>
                  <div className="flex items-center justify-between">
                    <span className={`text-sm font-medium ${
                      service.status === 'running' ? 'text-green-400' : 
                      service.status === 'error' ? 'text-red-400' : 'text-yellow-400'
                    }`}>
                      {service.status.charAt(0).toUpperCase() + service.status.slice(1)}
                    </span>
                    {service.port && (
                      <a
                        href={`http://localhost:${service.port}`}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-blue-400 hover:text-blue-300 text-sm flex items-center space-x-1"
                      >
                        <Globe className="w-4 h-4" />
                        <span>Open</span>
                      </a>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </div>
        )}

        {activeTab === 'analytics' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white flex items-center">
              <BarChart3 className="w-6 h-6 mr-3" />
              Real-Time Analytics
            </h2>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                <h3 className="text-lg font-semibold text-white mb-4">Event Distribution</h3>
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <span className="text-slate-300">Insurance Claims</span>
                    <div className="flex items-center space-x-2">
                      <div className="w-24 bg-slate-700 rounded-full h-2">
                        <div className="bg-blue-500 h-2 rounded-full" style={{ width: '35%' }}></div>
                      </div>
                      <span className="text-slate-400 text-sm">35%</span>
                    </div>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-slate-300">Financial Transactions</span>
                    <div className="flex items-center space-x-2">
                      <div className="w-24 bg-slate-700 rounded-full h-2">
                        <div className="bg-green-500 h-2 rounded-full" style={{ width: '40%' }}></div>
                      </div>
                      <span className="text-slate-400 text-sm">40%</span>
                    </div>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-slate-300">IoT Telemetry</span>
                    <div className="flex items-center space-x-2">
                      <div className="w-24 bg-slate-700 rounded-full h-2">
                        <div className="bg-purple-500 h-2 rounded-full" style={{ width: '25%' }}></div>
                      </div>
                      <span className="text-slate-400 text-sm">25%</span>
                    </div>
                  </div>
                </div>
              </div>
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                <h3 className="text-lg font-semibold text-white mb-4">Risk Score Distribution</h3>
                <div className="space-y-4">
                  <div className="flex items-center justify-between">
                    <span className="text-green-300">Low Risk (0.0-0.3)</span>
                    <span className="text-slate-400">68.2%</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-yellow-300">Medium Risk (0.3-0.7)</span>
                    <span className="text-slate-400">24.1%</span>
                  </div>
                  <div className="flex items-center justify-between">
                    <span className="text-red-300">High Risk (0.7-1.0)</span>
                    <span className="text-slate-400">7.7%</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'testing' && (
          <div className="space-y-6">
            <h2 className="text-2xl font-bold text-white flex items-center">
              <Play className="w-6 h-6 mr-3" />
              Testing & Commands
            </h2>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                <h3 className="text-lg font-semibold text-white mb-4">Quick Test Commands</h3>
                <div className="space-y-4">
                  <div className="bg-slate-900/50 rounded-lg p-4">
                    <div className="text-slate-300 text-sm mb-2">Run complete quick test:</div>
                    <code className="text-green-400 text-sm">./scripts/quick-test.sh</code>
                  </div>
                  <div className="bg-slate-900/50 rounded-lg p-4">
                    <div className="text-slate-300 text-sm mb-2">Start data generation (50K events/sec):</div>
                    <code className="text-green-400 text-sm">python3 data-generators/producer.py --throughput 50000</code>
                  </div>
                  <div className="bg-slate-900/50 rounded-lg p-4">
                    <div className="text-slate-300 text-sm mb-2">Test infrastructure:</div>
                    <code className="text-green-400 text-sm">./scripts/test-infrastructure.sh</code>
                  </div>
                </div>
              </div>
              <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
                <h3 className="text-lg font-semibold text-white mb-4">Load Testing</h3>
                <div className="space-y-4">
                  <div className="bg-slate-900/50 rounded-lg p-4">
                    <div className="text-slate-300 text-sm mb-2">Scaling test:</div>
                    <code className="text-green-400 text-sm">python3 scripts/test-producer-load.py --test-type scaling</code>
                  </div>
                  <div className="bg-slate-900/50 rounded-lg p-4">
                    <div className="text-slate-300 text-sm mb-2">Endurance test (10 min):</div>
                    <code className="text-green-400 text-sm">python3 scripts/test-producer-load.py --test-type endurance --duration 600</code>
                  </div>
                </div>
              </div>
            </div>
            
            <div className="bg-slate-800/50 backdrop-blur-sm rounded-xl p-6 border border-slate-700">
              <h3 className="text-lg font-semibold text-white mb-4">Web Interfaces</h3>
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <a
                  href="http://localhost:3000"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center space-x-3 p-4 bg-orange-600/20 hover:bg-orange-600/30 rounded-lg border border-orange-500/30 transition-colors"
                >
                  <TrendingUp className="w-6 h-6 text-orange-400" />
                  <div>
                    <div className="text-white font-medium">Grafana</div>
                    <div className="text-slate-400 text-sm">admin/admin</div>
                  </div>
                </a>
                <a
                  href="http://localhost:5601"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center space-x-3 p-4 bg-pink-600/20 hover:bg-pink-600/30 rounded-lg border border-pink-500/30 transition-colors"
                >
                  <Database className="w-6 h-6 text-pink-400" />
                  <div>
                    <div className="text-white font-medium">Kibana</div>
                    <div className="text-slate-400 text-sm">Log Analysis</div>
                  </div>
                </a>
                <a
                  href="http://localhost:8086"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center space-x-3 p-4 bg-cyan-600/20 hover:bg-cyan-600/30 rounded-lg border border-cyan-500/30 transition-colors"
                >
                  <Cpu className="w-6 h-6 text-cyan-400" />
                  <div>
                    <div className="text-white font-medium">InfluxDB</div>
                    <div className="text-slate-400 text-sm">Time Series</div>
                  </div>
                </a>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;