import axios from 'axios';

// Configuration de base pour Axios
const API_BASE_URL = 'http://localhost:8000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
});

// Intercepteur pour gérer les erreurs
api.interceptors.response.use(
  response => response,
  error => {
    console.error('API Error:', error);
    return Promise.reject(error);
  }
);

// Fonctions d'API pour les sports
export const getSports = async () => {
  const response = await api.get('/sports');
  return response.data;
};

export const getSport = async (sportId) => {
  const response = await api.get(`/sports/${sportId}`);
  return response.data;
};

// Fonctions d'API pour les matchs
export const getMatchs = async (params = {}) => {
  const response = await api.get('/matchs', { params });
  return response.data;
};

export const getMatch = async (matchId) => {
  const response = await api.get(`/matchs/${matchId}`);
  return response.data;
};

// Fonctions d'API pour les utilisateurs
export const getUsers = async () => {
  const response = await api.get('/users');
  return response.data;
};

export const getUser = async (userId) => {
  const response = await api.get(`/users/${userId}`);
  return response.data;
};

// Fonctions d'API pour les notifications
export const getNotifications = async (userId) => {
  const params = userId ? { user_id: userId } : {};
  const response = await api.get('/notifications', { params });
  return response.data;
};

// Fonctions d'API pour les statistiques
export const getSportsStats = async () => {
  const response = await api.get('/stats/sports');
  return response.data;
};

export const getMatchsStats = async () => {
  const response = await api.get('/stats/matchs');
  return response.data;
};

// Fonctions d'API pour Airflow
export const getAirflowDags = async () => {
  const response = await api.get('/airflow/dags');
  return response.data;
};

export const triggerDag = async (dagId, runId = null) => {
  const response = await api.post('/airflow/trigger_dag', {
    dag_id: dagId,
    run_id: runId
  });
  return response.data;
};

// Fonction pour vérifier l'état des services
export const getServicesHealth = async () => {
  const response = await api.get('/healthcheck');
  return response.data;
};

export default api; 