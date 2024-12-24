package com.crio.xpoll.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.crio.xpoll.model.User;
import com.crio.xpoll.util.DatabaseConnection;

/**
 * Data Access Object (DAO) for managing users in the XPoll application.
 * Provides methods for creating and retrieving user information.
 */
public class UserDAO {

    private final DatabaseConnection databaseConnection;

    /**
     * Constructs a UserDAO with the specified DatabaseConnection.
     *
     * @param databaseConnection The DatabaseConnection to be used for database operations.
     */
    public UserDAO(DatabaseConnection databaseConnection) {
        if (databaseConnection == null) {
            throw new IllegalArgumentException("DatabaseConnection cannot be null");
        }
        this.databaseConnection = databaseConnection;
    
    }

    /**
     * Creates a new user with the specified username and password.
     *
     * @param username The username for the new user.
     * @param password The password for the new user.
     * @return A User object representing the created user.
     * @throws SQLException If a database error occurs during user creation.
     */
    public User createUser (String username, String password) throws SQLException {
        String sql = "INSERT INTO users (username, password) VALUES (?, ?)";
        
        try (Connection connection = databaseConnection.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS)) {
            
            preparedStatement.setString(1, username);
            preparedStatement.setString(2, password);
            
            int affectedRows = preparedStatement.executeUpdate();
            if (affectedRows == 0) {
                throw new SQLException("Creating user failed, no rows affected.");
            }
            
            // Get the generated user ID
            try (ResultSet generatedKeys = preparedStatement.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    int userId = generatedKeys.getInt(1);
                    return new User(userId, username, password); // Assuming User has a constructor with these parameters
                } else {
                    throw new SQLException("Creating user failed, no ID obtained.");
                }
            }
        }
    }
    

    /**
     * Retrieves a user by their ID.
     *
     * @param userId The ID of the user to retrieve.
     * @return A User object representing the user with the specified ID, or null if no user is found.
     * @throws SQLException If a database error occurs during the retrieval.
     */
    public User getUserById(int userId) throws SQLException {
        String sql = "SELECT * FROM users WHERE id = ?";
    try (Connection connection = databaseConnection.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
        
        preparedStatement.setInt(1, userId);
        
        try (ResultSet resultSet = preparedStatement.executeQuery()) {
            if (resultSet.next()) {
                String username = resultSet.getString("username");
                String password = resultSet.getString("password");
                return new User(userId, username, password); // Assuming User has a constructor with these parameters
            } else {
                return null; // User not found
            }
        }
    }
    }
}