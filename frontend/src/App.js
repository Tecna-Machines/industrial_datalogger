import React, { useState, useEffect } from 'react';
import { RefreshCw, Activity, AlertCircle, TrendingUp, Clock, Settings } from 'lucide-react';

function App() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [refreshInterval, setRefreshInterval] = useState(5);
  const [isPaused, setIsPaused] = useState(false);
  const [lastUpdate, setLastUpdate] = useState(null);

  const fetchData = async () => {
    try {
      const response = await fetch('/latest?n=10');
      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }
      const result = await response.json();
      setData(result.items || []);
      setLastUpdate(new Date());
      setError(null);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, []);

  useEffect(() => {
    if (isPaused) return;

    const interval = setInterval(() => {
      fetchData();
    }, refreshInterval * 1000);

    return () => clearInterval(interval);
  }, [refreshInterval, isPaused]);

  const formatDateTime = (dateString) => {
    if (!dateString) return 'N/A';
    const date = new Date(dateString);
    return date.toLocaleString('es-ES', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit'
    });
  };

  const getEstadoColor = (estado) => {
    switch (estado) {
      case 1: return 'bg-green-100 text-green-800';
      case 0: return 'bg-red-100 text-red-800';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  const getEstadoText = (estado) => {
    switch (estado) {
      case 1: return 'Activo';
      case 0: return 'Detenido';
      default: return 'Desconocido';
    }
  };

  const calculateOEE = (item) => {
    return item.OEE ? (parseFloat(item.OEE) * 100).toFixed(2) : '0.00';
  };

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Header */}
        <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
          <div className="flex items-center justify-between">
            <div className="flex items-center space-x-3">
              <Activity className="w-8 h-8 text-blue-600" />
              <div>
                <h1 className="text-2xl font-bold text-gray-900">Dashboard de Producción</h1>
                <p className="text-sm text-gray-500">Monitoreo en tiempo real - Últimos 10 registros</p>
              </div>
            </div>
            <div className="flex items-center space-x-4">
              {lastUpdate && (
                <div className="text-sm text-gray-500 flex items-center">
                  <Clock className="w-4 h-4 mr-1" />
                  Última actualización: {formatDateTime(lastUpdate)}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Controls */}
        <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex items-center space-x-4">
              <div className="flex items-center space-x-2">
                <Settings className="w-5 h-5 text-gray-600" />
                <label className="text-sm font-medium text-gray-700">
                  Intervalo de refresco (segundos):
                </label>
                <input
                  type="number"
                  min="1"
                  max="300"
                  value={refreshInterval}
                  onChange={(e) => setRefreshInterval(Math.max(1, parseInt(e.target.value) || 1))}
                  className="w-20 px-3 py-1 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <button
                onClick={() => setIsPaused(!isPaused)}
                className={`px-4 py-2 rounded-md font-medium transition-colors ${
                  isPaused 
                    ? 'bg-green-600 hover:bg-green-700 text-white' 
                    : 'bg-yellow-600 hover:bg-yellow-700 text-white'
                }`}
              >
                {isPaused ? 'Reanudar' : 'Pausar'}
              </button>
              <button
                onClick={fetchData}
                disabled={loading}
                className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md font-medium transition-colors disabled:opacity-50 flex items-center space-x-2"
              >
                <RefreshCw className={`w-4 h-4 ${loading ? 'loading-spinner' : ''}`} />
                <span>Actualizar ahora</span>
              </button>
            </div>
            <div className="text-sm text-gray-600">
              Registros mostrados: {data.length}
            </div>
          </div>
        </div>

        {/* Error Message */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4 mb-6">
            <div className="flex items-center">
              <AlertCircle className="w-5 h-5 text-red-600 mr-2" />
              <span className="text-red-800">Error: {error}</span>
            </div>
          </div>
        )}

        {/* Data Table */}
        <div className="bg-white rounded-lg shadow-sm overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-semibold text-gray-900 flex items-center">
              <TrendingUp className="w-5 h-5 mr-2 text-blue-600" />
              Registros de Producción
            </h2>
          </div>
          
          {loading ? (
            <div className="p-8 text-center">
              <RefreshCw className="w-8 h-8 loading-spinner mx-auto text-blue-600 mb-2" />
              <p className="text-gray-600">Cargando datos...</p>
            </div>
          ) : data.length === 0 ? (
            <div className="p-8 text-center text-gray-500">
              No hay datos disponibles
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">ID</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Fecha</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Producto</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Estado</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Turno</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Piezas Totales</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Piezas Good</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Piezas Bad</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">OEE %</th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Tiempo Producción</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {data.map((item, index) => (
                    <tr key={item.Id_func} className={index === 0 ? 'bg-blue-50' : ''}>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        {item.Id_func}
                        {index === 0 && (
                          <span className="ml-2 px-2 py-1 text-xs bg-blue-100 text-blue-800 rounded-full">Más reciente</span>
                        )}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {formatDateTime(item.Fecha)}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {item.Producto || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`px-2 py-1 text-xs font-medium rounded-full ${getEstadoColor(item.Estado)}`}>
                          {getEstadoText(item.Estado)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {item.turno || 'N/A'}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {item.PiezasTotales || 0}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {item.PiezasGood || 0}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {item.PiezasBad || 0}
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        {calculateOEE(item)}%
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                        {item.T_Producido || 'N/A'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Summary Cards */}
        {data.length > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mt-6">
            <div className="bg-white rounded-lg shadow-sm p-4">
              <div className="text-sm font-medium text-gray-500">Total Piezas</div>
              <div className="text-2xl font-bold text-gray-900">
                {data.reduce((sum, item) => sum + (item.PiezasTotales || 0), 0)}
              </div>
            </div>
            <div className="bg-white rounded-lg shadow-sm p-4">
              <div className="text-sm font-medium text-gray-500">Piezas Good</div>
              <div className="text-2xl font-bold text-green-600">
                {data.reduce((sum, item) => sum + (item.PiezasGood || 0), 0)}
              </div>
            </div>
            <div className="bg-white rounded-lg shadow-sm p-4">
              <div className="text-sm font-medium text-gray-500">Piezas Bad</div>
              <div className="text-2xl font-bold text-red-600">
                {data.reduce((sum, item) => sum + (item.PiezasBad || 0), 0)}
              </div>
            </div>
            <div className="bg-white rounded-lg shadow-sm p-4">
              <div className="text-sm font-medium text-gray-500">OEE Promedio</div>
              <div className="text-2xl font-bold text-blue-600">
                {(data.reduce((sum, item) => sum + (parseFloat(item.OEE) || 0), 0) / data.length * 100).toFixed(2)}%
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
