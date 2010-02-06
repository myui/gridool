/*
 * @(#)$Id$
 *
 * Copyright 2006-2008 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * Contributors:
 *     Makoto YUI - initial implementation
 */
package gridool.db.sql;

import static java.io.StreamTokenizer.TT_EOF;
import static java.io.StreamTokenizer.TT_WORD;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;

import javax.annotation.Nonnull;

import xbird.util.io.IOUtils;

/**
 * 
 * <DIV lang="en"></DIV>
 * <DIV lang="ja"></DIV>
 * 
 * @author Makoto YUI (yuin405@gmail.com)
 */
public final class SQLTranslator {

    private static final int TT_COMMA = ',';
    private static final int TT_LPAR = '(';
    private static final int TT_RPAR = ')';
    private static final int TT_QUOTE = '\'';
    private static final int TT_DQUOTE = '\"';

    public SQLTranslator() {}

    @Nonnull
    public String translateSelect(@Nonnull final String query) {
        if(!query.contains("select") && !query.contains("SELECT")) {
            return null;
        }
        final StreamTokenizer lexer = new StreamTokenizer(new StringReader(query));
        lexer.resetSyntax();
        lexer.wordChars('a', 'z');
        lexer.wordChars('A', 'Z');
        lexer.wordChars(128 + 32, 255);
        lexer.wordChars('0', '9');
        lexer.wordChars('.', '.'); // digit
        //lexer.wordChars('-', '-'); // digit
        lexer.wordChars('_', '_');
        lexer.whitespaceChars(0, ' ');
        lexer.whitespaceChars(' ', ' ');
        lexer.whitespaceChars('\t', '\t');
        lexer.whitespaceChars('\n', '\n');
        lexer.whitespaceChars('\r', '\r');
        lexer.quoteChar(TT_QUOTE);
        lexer.quoteChar(TT_DQUOTE);
        lexer.eolIsSignificant(false);

        final StringBuilder queryBuf = new StringBuilder(query.length());
        try {
            parseQuery(lexer, queryBuf);
        } catch (IOException e) {
            e.printStackTrace();
            return query;
        }
        return queryBuf.toString();
    }

    private static void parseQuery(StreamTokenizer lexer, StringBuilder queryBuf)
            throws IOException {
        int tt;
        while((tt = lexer.nextToken()) != TT_EOF) {
            if(tt != TT_WORD) {
                queryBuf.append((char) tt);
                continue;
            }
            final String token = lexer.sval;
            final char c = token.charAt(0);
            switch(c) {
                case 's':
                case 'S':
                    if("select".equalsIgnoreCase(token)) {
                        queryBuf.append("SELECT ");
                        parseSelect(lexer, queryBuf);
                        break;
                    }
                    // fall through
                default:
                    queryBuf.append(token);
                    queryBuf.append(' ');
                    break;
            }
        }
    }

    private static void parseSelect(StreamTokenizer lexer, StringBuilder queryBuf)
            throws IOException {
        int tt;
        String prevToken = null;
        boolean insertSpace = false;
        while((tt = lexer.nextToken()) != TT_EOF) {
            if(tt != TT_WORD) {
                if(tt == TT_QUOTE || tt == TT_DQUOTE) {
                    queryBuf.append(' ');
                    queryBuf.append((char) tt);
                    queryBuf.append(lexer.sval);
                    queryBuf.append((char) tt);
                    insertSpace = true;
                } else {
                    queryBuf.append((char) tt);
                    insertSpace = (tt == TT_COMMA || tt == TT_RPAR);
                }
                continue;
            }
            final String token = lexer.sval;
            final char c = token.charAt(0);
            switch(c) {
                case 'p':
                case 'P':
                    if("partitioned".equalsIgnoreCase(token)) {
                        tryParsePartitionBy(lexer, queryBuf, prevToken);
                        break;
                    }
                    // fall through
                default:
                    if(insertSpace) {
                        queryBuf.append(' ');
                    }
                    queryBuf.append(token);
                    insertSpace = true;
                    break;
            }
            prevToken = token;
        }
    }

    private static void tryParsePartitionBy(StreamTokenizer lexer, StringBuilder queryBuf, String tableName)
            throws IOException {
        int tt = lexer.nextToken();
        if(tt != TT_WORD) {
            queryBuf.append("partition ");
            queryBuf.append((char) tt);
            return;
        }
        String token = lexer.sval;
        if(!"by".equalsIgnoreCase(token)) {
            queryBuf.append(token);
            queryBuf.append(' ');
            return;
        }
        // partitioned by
        if((tt = lexer.nextToken()) == TT_EOF) {
            return;
        }
        if(tt != TT_WORD) {
            queryBuf.append((char) tt);
            return;
        }
        int bitset = 0;
        String field = lexer.sval;
        if("primary".equalsIgnoreCase(field)) {
            tt = lexer.nextToken();
            if(tt != TT_WORD && "key".equalsIgnoreCase(lexer.sval)) {
                throw new IllegalStateException("Syntax error: " + lexer.toString());
            }
            // partitioned by primary key
            bitset = 1;
        } else {
            // partitioned by FIELD
            while((tt = lexer.nextToken()) != TT_EOF) {
                if(tt != TT_WORD) {
                    queryBuf.append((char) tt);
                    break;
                }
                if(!"and".equalsIgnoreCase(lexer.sval)) {
                    lexer.pushBack();
                    break;
                }
                tt = lexer.nextToken();
                if(tt != TT_WORD) {
                    throw new IllegalStateException("Syntax error: " + lexer.toString());
                }
                // partitioned by FIELD (and FIELD)* 
            }
        }

    }

    public static void main(String[] args) throws FileNotFoundException, IOException {
        File file = new File("/home/myui/workspace/gridool/main/test/java/sqlparse/tpch/01.sql");
        String query = IOUtils.toString(new FileInputStream(file));
        SQLTranslator translator = new SQLTranslator();
        translator.translateSelect(query);
    }
}
