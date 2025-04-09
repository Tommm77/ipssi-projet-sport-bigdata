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
  Chip,
  Avatar
} from '@mui/material';

// Importer le service API
import { getUsers } from '../services/api';

const UsersPage = () => {
  const [loading, setLoading] = useState(true);
  const [users, setUsers] = useState([]);

  useEffect(() => {
    const fetchUsers = async () => {
      try {
        // Récupérer les données depuis l'API
        const data = await getUsers();
        setUsers(data);
        setLoading(false);
      } catch (error) {
        console.error("Erreur lors du chargement des utilisateurs:", error);
        setLoading(false);
      }
    };

    fetchUsers();
  }, []);

  const sportIdToName = {
    1: "Football",
    2: "Basketball",
    3: "Tennis",
    4: "Rugby",
    5: "Volleyball"
  };

  return (
    <Box>
      <Typography variant="h4" gutterBottom>
        Utilisateurs
      </Typography>
      
      {loading ? (
        <Typography>Chargement des utilisateurs...</Typography>
      ) : (
        <TableContainer component={Paper}>
          <Table>
            <TableHead>
              <TableRow>
                <TableCell>Utilisateur</TableCell>
                <TableCell>Email</TableCell>
                <TableCell>Date d'inscription</TableCell>
                <TableCell>Sports favoris</TableCell>
                <TableCell>Notifications</TableCell>
              </TableRow>
            </TableHead>
            <TableBody>
              {users.map((user) => (
                <TableRow key={user.user_id}>
                  <TableCell>
                    <Box sx={{ display: 'flex', alignItems: 'center' }}>
                      <Avatar sx={{ mr: 2 }}>
                        {user.prenom?.charAt(0) || ''}{user.nom?.charAt(0) || ''}
                      </Avatar>
                      <Typography>
                        {user.prenom} {user.nom}
                      </Typography>
                    </Box>
                  </TableCell>
                  <TableCell>{user.email}</TableCell>
                  <TableCell>
                    {new Date(user.date_inscription).toLocaleDateString()}
                  </TableCell>
                  <TableCell>
                    <Box sx={{ display: 'flex', flexWrap: 'wrap', gap: 0.5 }}>
                      {user.sports_favoris?.map(sportId => (
                        <Chip 
                          key={sportId} 
                          label={sportIdToName[sportId] || `Sport ${sportId}`} 
                          size="small" 
                          color="primary" 
                          variant="outlined"
                        />
                      ))}
                    </Box>
                  </TableCell>
                  <TableCell>
                    <Chip 
                      label={user.notification_active ? "Activées" : "Désactivées"} 
                      color={user.notification_active ? "success" : "default"} 
                      size="small" 
                    />
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </TableContainer>
      )}
    </Box>
  );
};

export default UsersPage; 