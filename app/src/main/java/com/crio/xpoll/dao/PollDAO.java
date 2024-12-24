package com.crio.xpoll.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import com.crio.xpoll.model.Choice;
import com.crio.xpoll.model.Poll;
import com.crio.xpoll.model.PollSummary;
import com.crio.xpoll.util.DatabaseConnection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Data Access Object (DAO) for managing polls in the XPoll application.
 * Provides methods for creating, retrieving, closing polls, and fetching poll summaries.
 */
public class PollDAO {

    private final DatabaseConnection databaseConnection;

    /**
     * Constructs a PollDAO with the specified DatabaseConnection.
     *
     * @param databaseConnection The DatabaseConnection to be used for database operations.
     */
    public PollDAO(DatabaseConnection databaseConnection) {
        this.databaseConnection = databaseConnection;
    }

    /**
     * Creates a new poll with the specified question and choices.
     *
     * @param userId   The ID of the user creating the poll.
     * @param question The question for the poll.
     * @param choices  A list of choices for the poll.
     * @return The created Poll object with its associated choices.
     * @throws SQLException If a database error occurs during poll creation.
     */
    public Poll createPoll(int userId, String question, List<String> choices) throws SQLException {
        String pollSql = "INSERT INTO polls (user_id, question) VALUES (?, ?)";
        int pollId;

        try (Connection conn = databaseConnection.getConnection();
                PreparedStatement pollStmt =
                        conn.prepareStatement(pollSql, Statement.RETURN_GENERATED_KEYS)) {

            pollStmt.setInt(1, userId);
            pollStmt.setString(2, question);
            pollStmt.executeUpdate();

            try (ResultSet generatedKeys = pollStmt.getGeneratedKeys()) {
                if (generatedKeys.next()) {
                    pollId = generatedKeys.getInt(1);
                } else {
                    throw new SQLException("Creating poll failed, no ID obtained.");
                }
            }
        }

        List<Choice> choiceList = new ArrayList<>();
        String choiceSql = "INSERT INTO choices (poll_id, choice_text) VALUES (?, ?)";

        try (Connection conn = databaseConnection.getConnection();
                PreparedStatement choiceStmt =
                        conn.prepareStatement(choiceSql, Statement.RETURN_GENERATED_KEYS)) {

            for (String choiceText : choices) {
                choiceStmt.setInt(1, pollId);
                choiceStmt.setString(2, choiceText);
                choiceStmt.executeUpdate();

                try (ResultSet generatedKeys = choiceStmt.getGeneratedKeys()) {
                    if (generatedKeys.next()) {
                        int generatedChoiceId = generatedKeys.getInt(1);
                        choiceList.add(new Choice(generatedChoiceId, pollId, choiceText));
                    } else {
                        throw new SQLException("Creating choice failed, no ID obtained.");
                    }
                }
            }
        }

        return new Poll(pollId, userId, question, choiceList);
    }

    /**
     * Retrieves a poll by its ID.
     *
     * @param pollId The ID of the poll to retrieve.
     * @return The Poll object with its associated choices.
     * @throws SQLException If a database error occurs or the poll is not found.
     */
    public Poll getPoll(int pollId) throws SQLException {
        String pollSql = "SELECT user_id, question FROM polls WHERE id = ?";
        String choiceSql = "SELECT choice_text FROM choices WHERE poll_id = ?";

        try (Connection conn = databaseConnection.getConnection();
                PreparedStatement pollStmt = conn.prepareStatement(pollSql);
                PreparedStatement choiceStmt = conn.prepareStatement(choiceSql)) {


            pollStmt.setInt(1, pollId);
            Poll poll = null;

            try (ResultSet pollRs = pollStmt.executeQuery()) {
                if (pollRs.next()) {
                    int userId = pollRs.getInt("user_id");
                    String question = pollRs.getString("question");
                    poll = new Poll(pollId, userId, question, new ArrayList<>());
                    poll.setClosed(true);
                } else {
                    return null;
                }
            }


            choiceStmt.setInt(1, pollId);
            try (ResultSet choiceRs = choiceStmt.executeQuery()) {
                int choiceId = 1;
                while (choiceRs.next()) {
                    String choiceText = choiceRs.getString("choice_text");
                    poll.getChoices().add(new Choice(choiceId, pollId, choiceText));
                    choiceId++;
                }
            }

            return poll;
        } catch (SQLException e) {
            System.err.println("Error while fetching poll data with pollId " + pollId + ": " + e.getMessage());
            e.printStackTrace();
            throw new SQLException("Error while fetching poll data with pollId " + pollId, e);
        }
    }

    /**
     * Closes a poll by updating its status in the database.
     *
     * @param pollId The ID of the poll to close.
     * @throws SQLException If a database error occurs during the update.
     */
    public void closePoll(int pollId) throws SQLException {
        String query = "UPDATE polls SET is_closed = ? WHERE id = ?";
        try (Connection conn = databaseConnection.getConnection();
             PreparedStatement stmt = conn.prepareStatement(query)) {

            stmt.setBoolean(1, true);
            stmt.setInt(2, pollId);
            stmt.executeUpdate();
        }
    }

    /**
     * Retrieves a list of poll summaries for the specified poll.
     *
     * @param pollId The ID of the poll for which to retrieve summaries.
     * @return A list of PollSummary objects containing the poll question, choice text, and response count.
     * @throws SQLException If a database error occurs during the query.
     */
    public List<PollSummary> getPollSummaries(int pollId) throws SQLException {
        List<PollSummary> summaries = new ArrayList<>();
        String sql = "SELECT p.question, c.choice_text, COUNT(r.choice_id) AS response_count "
                + "FROM polls p " + "JOIN choices c ON p.id = c.poll_id "
                + "LEFT JOIN responses r ON c.id = r.choice_id " + "WHERE p.id = ? GROUP BY c.id";

        try (Connection conn = databaseConnection.getConnection();
                PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, pollId);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    String question = rs.getString("question");
                    String choiceText = rs.getString("choice_text");
                    int responseCount = rs.getInt("response_count");
                    summaries.add(new PollSummary(question, choiceText, responseCount));
                }
            }
        }
        return summaries;

    }
}