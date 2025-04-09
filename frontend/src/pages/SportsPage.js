import React, { useState, useEffect } from 'react';
import {
  Typography,
  Box,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Chip
} from '@mui/material';

// Importer le service API
import { getSports } from '../services/api';

const SportsPage = () => {
  const [loading, setLoading] = useState(true);
  const [sports, setSports] = useState([]);

  useEffect(() => {
    const fetchSports = async () => {
      try {
        // Récupérer les données depuis l'API
        const data = await getSports();
        setSports(data);
        setLoading(false);
      } catch (error) {
        console.error("Erreur lors du chargement des sports:", error);
        setLoading(false);
      }
    };

    fetchSports();
  }, []);

  const getCategorieColor = (categorie) => {
    switch (categorie.toLowerCase()) {
      case 'collectif':
        return 'primary';
      case 'individuel':
        return 'secondary';
      default:
        return 'default';
    }
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Sports
      </Typography>
      
      {loading ? (
        <Typography>Chargement des sports...</Typography>
      ) : (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>ID</TableCell>
                <TableCell>Nom</TableCell>
                <TableCell>Catégorie</TableCell>
                <TableCell>Nombre de joueurs</TableCell>
                <TableCell>Description</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {sports.map((sport) => (
                <TableRow key={sport.sport_id}>
                  <TableCell>{sport.sport_id}</TableCell>
                  <TableCell>{sport.nom}</TableCell>
                  <TableCell>
                    <Chip 
                      label={sport.categorie} 
                      color={getCategorieColor(sport.categorie)} 
                      size="small" 
                    />
                  </TableCell>
                  <TableCell>{sport.nombre_joueurs}</TableCell>
                  <TableCell>{sport.description}</TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
};

export default SportsPage; 