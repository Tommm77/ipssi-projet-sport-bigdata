import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  Paper,
  Grid,
  Button,
  Card,
  CardContent,
  CardActions,
  Chip,
  List,
  ListItem,
  ListItemText,
  Divider,
  Alert
} from '@mui/material';
import PlayArrowIcon from '@mui/icons-material/PlayArrow';
import HistoryIcon from '@mui/icons-material/History';
import ErrorIcon from '@mui/icons-material/Error';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import LoopIcon from '@mui/icons-material/Loop';

// Importer les services API
import { getAirflowDags, triggerDag, getServicesHealth } from '../services/api';

const PipelinePage = () => {
  const [loading, setLoading] = useState(true);
  const [dags, setDags] = useState([]);
  const [runningDag, setRunningDag] = useState(null);
  const [servicesStatus, setServicesStatus] = useState({});
  const [executionLog, setExecutionLog] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      try {
        // Récupérer les données depuis les APIs
        const dagsData = await getAirflowDags();
        const statusData = await getServicesHealth();
        
        // Données simulées pour les logs (à remplacer par une API réelle)
        const mockLogs = [
          { timestamp: "2023-11-15 14:30:00", message: "Pipeline sports_data_pipeline démarré", level: "info" },
          { timestamp: "2023-11-15 14:30:05", message: "Tâche generate_sports terminée avec succès", level: "success" },
          { timestamp: "2023-11-15 14:30:20", message: "Tâche generate_matchs terminée avec succès", level: "success" },
          { timestamp: "2023-11-15 14:31:00", message: "Tâche check_spark_processing terminée avec succès", level: "success" },
          { timestamp: "2023-11-15 14:32:10", message: "Tâche run_hive_queries terminée avec succès", level: "success" },
          { timestamp: "2023-11-15 14:32:15", message: "Pipeline sports_data_pipeline terminé avec succès", level: "success" },
          { timestamp: "2023-11-15 15:00:00", message: "Pipeline match_data_sync démarré", level: "info" },
          { timestamp: "2023-11-15 15:00:10", message: "Tâche fetch_new_matches en cours d'exécution...", level: "info" }
        ];
        
        // Détecter un DAG en cours d'exécution
        const runningDagInfo = dagsData.find(dag => dag.status === "running");
        
        setDags(dagsData);
        setServicesStatus(statusData);
        setExecutionLog(mockLogs);
        if (runningDagInfo) {
          setRunningDag(runningDagInfo.dag_id);
        }
        setLoading(false);
      } catch (error) {
        console.error("Erreur lors du chargement des données:", error);
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  const handleTriggerDag = async (dagId) => {
    try {
      // Appeler l'API pour déclencher le DAG
      await triggerDag(dagId);
      
      // Mettre à jour l'interface
      setExecutionLog(prev => [
        { 
          timestamp: new Date().toLocaleString(), 
          message: `Pipeline ${dagId} déclenché manuellement`, 
          level: "info" 
        },
        ...prev
      ]);
      
      // Mettre à jour le statut du DAG
      setDags(prev => prev.map(dag => 
        dag.dag_id === dagId 
          ? { ...dag, status: "running", last_run: new Date().toLocaleString() }
          : dag
      ));
      
      setRunningDag(dagId);
    } catch (error) {
      console.error(`Erreur lors du déclenchement du DAG ${dagId}:`, error);
      // Ajouter un message d'erreur aux logs
      setExecutionLog(prev => [
        { 
          timestamp: new Date().toLocaleString(), 
          message: `Erreur lors du déclenchement du DAG ${dagId}: ${error.message}`, 
          level: "error" 
        },
        ...prev
      ]);
    }
  };

  const getStatusColor = (status) => {
    switch (status.toLowerCase()) {
      case 'success':
        return 'success';
      case 'running':
        return 'primary';
      case 'failed':
        return 'error';
      default:
        return 'default';
    }
  };

  const getStatusIcon = (status) => {
    switch (status.toLowerCase()) {
      case 'success':
        return <CheckCircleIcon color="success" />;
      case 'running':
        return <LoopIcon color="primary" />;
      case 'failed':
        return <ErrorIcon color="error" />;
      default:
        return <HistoryIcon />;
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Pipeline de données
      </Typography>
      
      {/* Statut des services */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Typography variant="h6" gutterBottom>
          Statut des services
        </Typography>
        <Grid container spacing={2}>
          {Object.entries(servicesStatus).map(([service, status]) => (
            <Grid item key={service}>
              <Chip
                label={`${service}: ${status}`}
                color={status === 'ok' ? 'success' : 'error'}
                variant="outlined"
              />
            </Grid>
          ))}
        </Grid>
      </Paper>
      
      {/* DAGs Airflow */}
      <Grid container spacing={3}>
        <Grid item xs={12} md={7}>
          <Typography variant="h6" gutterBottom>
            Pipelines disponibles
          </Typography>
          <Grid container spacing={2}>
            {dags.map((dag) => (
              <Grid item xs={12} sm={6} key={dag.dag_id}>
                <Card>
                  <CardContent>
                    <Typography variant="h6">
                      {dag.dag_id}
                      {dag.status && (
                        <Chip
                          size="small"
                          label={dag.status}
                          color={getStatusColor(dag.status)}
                          icon={getStatusIcon(dag.status)}
                          sx={{ ml: 1 }}
                        />
                      )}
                    </Typography>
                    <Typography variant="body2" color="text.secondary">
                      {dag.description}
                    </Typography>
                    {dag.last_run && (
                      <Typography variant="caption" display="block" sx={{ mt: 1 }}>
                        Dernière exécution: {dag.last_run}
                      </Typography>
                    )}
                  </CardContent>
                  <CardActions>
                    <Button
                      size="small"
                      startIcon={<PlayArrowIcon />}
                      onClick={() => handleTriggerDag(dag.dag_id)}
                      disabled={dag.status === 'running' || !dag.is_active}
                    >
                      Exécuter
                    </Button>
                  </CardActions>
                </Card>
              </Grid>
            ))}
          </Grid>
        </Grid>
        
        {/* Logs d'exécution */}
        <Grid item xs={12} md={5}>
          <Paper sx={{ p: 2, height: '100%' }}>
            <Typography variant="h6" gutterBottom>
              Logs d'exécution
            </Typography>
            {runningDag && (
              <Alert severity="info" sx={{ mb: 2 }}>
                Le pipeline {runningDag} est en cours d'exécution
              </Alert>
            )}
            <List sx={{ 
              maxHeight: '400px', 
              overflow: 'auto',
              bgcolor: '#f5f5f5',
              border: '1px solid #ddd',
              borderRadius: 1
            }}>
              {executionLog.map((log, index) => (
                <React.Fragment key={index}>
                  <ListItem alignItems="flex-start">
                    <ListItemText
                      primary={log.message}
                      secondary={log.timestamp}
                      primaryTypographyProps={{
                        color: log.level === 'error' ? 'error' : 
                               log.level === 'success' ? 'success.main' : 
                               'text.primary'
                      }}
                    />
                  </ListItem>
                  {index < executionLog.length - 1 && <Divider component="li" />}
                </React.Fragment>
              ))}
            </List>
          </Paper>
        </Grid>
      </Grid>
    </Box>
  );
};

export default PipelinePage; 