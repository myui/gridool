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
import gridool.GridException;
import gridool.db.catalog.DistributionCatalog;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import xbird.util.string.StringUtils;

/**
 * 
 * <DIV lang="en">
 * partitioned_by (primarykey)
 * partitioned_by (field1, field2)
 * </DIV>
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
    private static final int TT_SEMICOLON = ';';

    @Nonnull
    private final DistributionCatalog catalog;

    public SQLTranslator(@CheckForNull DistributionCatalog catalog) {
        if(catalog == null) {
            throw new IllegalArgumentException();
        }
        this.catalog = catalog;
    }

    /**
     * @link http://www.asciitable.com/
     */
    @Nonnull
    public String translateQuery(@Nonnull final String query) throws GridException {
        final String trimedQuery = query.trim();
        if(!trimedQuery.contains("partitioned by") && !trimedQuery.contains("PARTITIONED BY")) {
            return trimedQuery;
        }
        final StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(trimedQuery));
        tokenizer.resetSyntax();
        tokenizer.wordChars('a', 'z');
        tokenizer.wordChars('A', 'Z');
        tokenizer.wordChars(128 + 32, 255);
        tokenizer.wordChars('0', '9');
        tokenizer.wordChars('.', '.'); // digit
        //tokenizer.wordChars('-', '-'); // digit
        tokenizer.wordChars('_', '_');
        tokenizer.wordChars('*', '*');
        tokenizer.whitespaceChars(0, ' ');
        //tokenizer.whitespaceChars(' ', ' ');
        tokenizer.whitespaceChars('\t', '\t');
        tokenizer.whitespaceChars('\n', '\n');
        tokenizer.whitespaceChars('\r', '\r');
        tokenizer.quoteChar(TT_QUOTE);
        tokenizer.quoteChar(TT_DQUOTE);
        tokenizer.eolIsSignificant(true);

        final StringBuilder queryBuf = new StringBuilder(query.length());
        try {
            parseQuery(tokenizer, queryBuf);
        } catch (IOException e) {
            throw new GridException(e);
        } catch (SQLException e) {
            throw new GridException(e);
        }
        return queryBuf.toString();
    }

    private void parseQuery(StreamTokenizer tokenizer, StringBuilder queryBuf) throws IOException,
            SQLException {
        int tt;
        String prevToken = null;
        boolean insertSpace = false;
        while((tt = tokenizer.nextToken()) != TT_EOF) {
            if(tt != TT_WORD) {
                if(tt == TT_QUOTE || tt == TT_DQUOTE) {
                    queryBuf.append(' ');
                    queryBuf.append((char) tt);
                    queryBuf.append(tokenizer.sval);
                    queryBuf.append((char) tt);
                    insertSpace = true;
                } else {
                    queryBuf.append((char) tt);
                    insertSpace = (tt == TT_COMMA || tt == TT_RPAR);
                }
                continue;
            }
            final String token = tokenizer.sval;
            final char c = token.charAt(0);
            switch(c) {
                case 'p':
                case 'P':
                    if("partitioned".equalsIgnoreCase(token)) {
                        int nextTT = tokenizer.nextToken();
                        String nextToken = tokenizer.sval;
                        if(nextTT == TT_WORD && "by".equalsIgnoreCase(nextToken)) {
                            if(insertSpace) {
                                queryBuf.append(' ');
                            }
                            parsePartitionedBy(tokenizer, queryBuf, prevToken);
                        } else {
                            if(insertSpace) {
                                queryBuf.append(' ');
                            }
                            queryBuf.append("partitioned ");
                            queryBuf.append(nextToken);

                        }
                        insertSpace = true;
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

    private void parsePartitionedBy(StreamTokenizer tokenizer, StringBuilder queryBuf, String tableName)
            throws IOException, SQLException {
        if(tableName == null) {
            throw new IllegalStateException("Syntax error: " + tokenizer.toString());
        }
        queryBuf.setLength(queryBuf.lastIndexOf(tableName));

        if(tokenizer.nextToken() != TT_LPAR) {
            throw new IllegalStateException("Syntax error: " + tokenizer.toString());
        }

        if(tokenizer.nextToken() != TT_WORD) {
            throw new IllegalStateException("Syntax error: " + tokenizer.toString());
        }
        String fieldName = tokenizer.sval;
        int bitset = 0;
        if("primarykey".equalsIgnoreCase(fieldName)) {
            bitset = 1;
            if(tokenizer.nextToken() != TT_RPAR) {
                throw new IllegalStateException("Syntax error: " + tokenizer.toString());
            }
        } else {
            final List<String> partByColumns = new ArrayList<String>(4);
            partByColumns.add(fieldName);
            int partitionKey = 0; // FIXME catalog.getPartitioningKey(tableName);
            bitset |= partitionKey;
            int lastTT;
            while((lastTT = tokenizer.nextToken()) == TT_COMMA) {
                if(tokenizer.nextToken() != TT_WORD) {
                    throw new IllegalStateException("Syntax error: " + tokenizer.toString());
                }
                fieldName = tokenizer.sval;
                partByColumns.add(fieldName);
                partitionKey = 0; // FIXME catalog.getPartitioningKey(tableName);
                bitset |= partitionKey;
            }
            if(lastTT != TT_RPAR) {
                throw new IllegalStateException("Syntax error: " + tokenizer.toString());
            }
        }

        if(tokenizer.nextToken() == TT_WORD && "alias".equalsIgnoreCase(tokenizer.sval)) {
            if(tokenizer.nextToken() != TT_WORD) {
                throw new IllegalStateException("Syntax error: " + tokenizer.toString());
            }
            tableName = tokenizer.sval;
        } else {
            tokenizer.pushBack();
        }

        queryBuf.append("(\"");
        queryBuf.append(tableName);
        queryBuf.append("\".\"");
        queryBuf.append(DistributionCatalog.hiddenFieldName);
        queryBuf.append("\" & ");
        queryBuf.append(bitset);
        queryBuf.append(") = ");
        queryBuf.append(bitset);
    }

    @Nonnull
    public static QueryString[] divideQuery(@Nonnull final String query, final boolean verifyAsSelect) {
        final List<QueryString> queryList = new ArrayList<QueryString>(4);
        final StringBuilder buf = new StringBuilder(query.length());

        final StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(query));
        tokenizer.resetSyntax();
        tokenizer.wordChars(0, TT_SEMICOLON - 1); // set everything excepting ";" as wordChars
        tokenizer.wordChars(TT_SEMICOLON + 1, 255); // set everything excepting ";" as wordChars
        tokenizer.quoteChar(TT_QUOTE);
        tokenizer.quoteChar(TT_DQUOTE);
        int tt;
        try {
            while((tt = tokenizer.nextToken()) != TT_EOF) {
                if(tt == TT_WORD) {
                    String token = tokenizer.sval;
                    buf.append(token);
                } else if(tt == TT_QUOTE || tt == TT_DQUOTE) {
                    char quote = (char) tt;
                    buf.append(quote);
                    buf.append(tokenizer.sval);
                    buf.append(quote);
                } else {
                    if(tt != TT_SEMICOLON) {
                        throw new IllegalStateException("SemiColon is expected but was "
                                + ((char) tt));
                    }
                    if(buf.length() > 0) {
                        String q = buf.toString().trim();
                        if(!q.isEmpty()) {
                            queryList.add(new QueryString(q));
                        }
                        StringUtils.clear(buf);
                    }
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to parse a query: " + query, e);
        }
        if(buf.length() > 0) {
            String q = buf.toString().trim();
            if(!q.isEmpty()) {
                queryList.add(new QueryString(q));
            }
        }

        final int numStmts = queryList.size();
        if(numStmts == 0) {
            throw new IllegalArgumentException("Illegal query: " + query);
        } else if(numStmts == 1) {
            return new QueryString[] { new QueryString(query) };
        }

        final QueryString[] queries = new QueryString[numStmts];
        queryList.toArray(queries);

        if(verifyAsSelect) {
            if(!inValidSelectQuery(queries)) {
                throw new IllegalArgumentException("Invalid as a SELECT query: " + query
                        + "\n(DDL)* Select (DDL)* is accepted here.");
            }
        }

        return queries;
    }

    /**
     * (DDL)* Select (DDL)* is accepted.
     */
    private static boolean inValidSelectQuery(final QueryString[] qstrs) {
        boolean foundSelect = false;
        for(final QueryString qs : qstrs) {
            if(qs.isSelect()) {
                if(foundSelect) {
                    return false;
                }
                foundSelect = true;
            } else {
                if(!qs.isDDL()) {
                    return false;
                }
            }
        }
        return true;
    }

    public static String selectFirstSelectQuery(final QueryString[] queries) {
        String selectQuery = null;
        for(QueryString qs : queries) {
            if(qs.isSelect()) {
                selectQuery = qs.getQuery();
                break;
            }
        }
        if(selectQuery == null) {
            throw new IllegalStateException("Select query is not found: "
                    + Arrays.toString(queries));
        }
        return selectQuery;
    }

    static final class QueryString {

        private final String query;
        private final boolean isSelect;
        private final boolean isDDL;

        QueryString(@CheckForNull String query) {
            if(query == null) {
                throw new IllegalArgumentException();
            }
            this.query = query;
            String lcQuery = query.toLowerCase();
            this.isSelect = lcQuery.startsWith("select");
            this.isDDL = isDDL(lcQuery);
        }

        @Nonnull
        String getQuery() {
            return query;
        }

        boolean isSelect() {
            return isSelect;
        }

        boolean isDDL() {
            return isDDL;
        }

        private static boolean isDDL(final String query) {
            return query.startsWith("create") || query.startsWith("drop");
        }

        @Override
        public String toString() {
            return query;
        }

    }

}
