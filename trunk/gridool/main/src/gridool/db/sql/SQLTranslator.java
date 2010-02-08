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
import gridool.db.catalog.DistributionCatalog;

import java.io.IOException;
import java.io.StreamTokenizer;
import java.io.StringReader;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

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

    @Nonnull
    private final DistributionCatalog catalog;

    public SQLTranslator(@CheckForNull DistributionCatalog catalog) {
        if(catalog == null) {
            throw new IllegalArgumentException();
        }
        this.catalog = catalog;
    }

    @Nonnull
    public String translateQuery(@Nonnull final String query) {
        if(!query.contains("partitioned by") && !query.contains("PARTITIONED BY")) {
            return query;
        }

        final StreamTokenizer tokenizer = new StreamTokenizer(new StringReader(query));
        tokenizer.resetSyntax();
        tokenizer.wordChars('a', 'z');
        tokenizer.wordChars('A', 'Z');
        tokenizer.wordChars(128 + 32, 255);
        tokenizer.wordChars('0', '9');
        tokenizer.wordChars('.', '.'); // digit
        //tokenizer.wordChars('-', '-'); // digit
        tokenizer.wordChars('_', '_');
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
            e.printStackTrace();
            return query;
        }
        return queryBuf.toString();
    }

    private void parseQuery(StreamTokenizer tokenizer, StringBuilder queryBuf) throws IOException {
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
            throws IOException {
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
            int partitionKey = catalog.getPartitioningKey(tableName, fieldName);
            bitset |= partitionKey;
            int lastTT;
            while((lastTT = tokenizer.nextToken()) == TT_COMMA) {
                if(tokenizer.nextToken() != TT_WORD) {
                    throw new IllegalStateException("Syntax error: " + tokenizer.toString());
                }
                fieldName = tokenizer.sval;
                partitionKey = catalog.getPartitioningKey(tableName, fieldName);
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

        queryBuf.append('(');
        queryBuf.append(tableName);
        queryBuf.append('.');
        queryBuf.append(DistributionCatalog.hiddenFieldName);
        queryBuf.append(" & ");
        queryBuf.append(bitset);
        queryBuf.append(") = ");
        queryBuf.append(bitset);
    }

}