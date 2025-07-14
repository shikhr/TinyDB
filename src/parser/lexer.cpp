#include "parser/lexer.h"
#include <cctype>
#include <stdexcept>

namespace tinydb
{

  Lexer::Lexer(const std::string &input) : m_input_(input) {}

  std::vector<Token> Lexer::tokenize()
  {
    std::vector<Token> tokens;
    while (!isEnd())
    {
      skipWhitespace();
      if (isEnd())
        break;

      char current = peek();
      if (std::isalpha(current) || current == '_')
      {
        tokens.push_back(readWord());
      }
      else if (current == '"')
      {
        tokens.push_back(readStringLiteral());
      }
      else if (std::isdigit(current))
      {
        tokens.push_back(readNumberLiteral());
      }
      else if (isOperator(current))
      {
        tokens.push_back(readOperator());
      }
      else if (isPunctuation(current))
      {
        tokens.push_back(readPunctuation());
      }
      else
      {
        m_has_error_ = true;
        m_error_message_ = "Unknown character: " + std::string(1, current);
        break;
      }
    }

    // Add END_OF_FILE token
    tokens.push_back({TokenType::END_OF_FILE, "", m_line_, m_column_});
    return tokens;
  }

  char Lexer::peek() const
  {
    if (isEnd())
      return '\0'; // End of input
    return m_input_[m_position_];
  }

  char Lexer::consume()
  {
    if (isEnd())
      throw std::runtime_error("Attempted to consume from an empty input.");
    char current = peek();
    if (current == '\n')
    {
      m_line_++;
      m_column_ = 1;
    }
    else
    {
      m_column_++;
    }
    m_position_++;
    return current;
  }

  Token Lexer::readWord()
  {
    std::string value;
    std::string normalised_value;
    while (std::isalnum(peek()) || peek() == '_')
    {
      char c = consume();
      value += c;
      normalised_value += std::toupper(c);
    }
    if (m_keywords_.count(normalised_value) > 0)
    {
      return {TokenType::KEYWORD, normalised_value, m_line_, m_column_};
    }

    return {TokenType::IDENTIFIER, value, m_line_, m_column_};
  }

  Token Lexer::readStringLiteral()
  {
    consume(); // Consume opening quote
    std::string value;
    while (peek() != '"' && !isEnd())
    {
      value += consume();
    }
    consume(); // Consume closing quote
    return {TokenType::STRING_LITERAL, value, m_line_, m_column_};
  }

  Token Lexer::readNumberLiteral()
  {
    std::string value;
    while (std::isdigit(peek()))
    {
      value += consume();
    }
    return {TokenType::NUMBER_LITERAL, value, m_line_, m_column_};
  }

  Token Lexer::readOperator()
  {
    char first = consume();
    std::string value(1, first);

    // Handle multi-character operators
    if (first == '=' && peek() == '=')
    {
      value += consume(); // ==
    }
    else if (first == '!' && peek() == '=')
    {
      value += consume(); // !=
    }
    else if (first == '<' && peek() == '=')
    {
      value += consume(); // <=
    }
    else if (first == '<' && peek() == '>')
    {
      value += consume(); // <>
    }
    else if (first == '>' && peek() == '=')
    {
      value += consume(); // >=
    }

    return {TokenType::OPERATOR, value, m_line_, m_column_};
  }

  Token Lexer::readPunctuation()
  {
    std::string value(1, consume());
    return {TokenType::PUNCTUATION, value, m_line_, m_column_};
  }

  void Lexer::skipWhitespace()
  {
    while (!isEnd() && std::isspace(peek()))
    {
      consume();
    }
  }

  bool Lexer::isEnd() const
  {
    return m_position_ >= m_input_.size();
  }

  bool Lexer::isPunctuation(char c) const
  {
    return std::string(".,;:()[]{}").find(c) != std::string::npos;
  }

  bool Lexer::isOperator(char c) const
  {
    return std::string("+-*/=<>!").find(c) != std::string::npos;
  }

} // namespace tinydb
