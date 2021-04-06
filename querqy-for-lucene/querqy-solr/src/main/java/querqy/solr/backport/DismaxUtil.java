package querqy.solr.backport;

import java.util.ArrayList;
import java.util.List;

/**
 * These utils are taken from protected code inside of the ExtendedDismaxParser.
 *
 * This provides for parsing out fields and boosts without putting terms thru analysis
 */
public class DismaxUtil {
    public static class Clause {

        boolean isBareWord() {
            return must==0 && !isPhrase;
        }

        public String field;
        public String rawField;  // if the clause is +(foo:bar) then rawField=(foo
        public boolean isPhrase;
        public boolean hasWhitespace;
        public boolean hasSpecialSyntax;
        public boolean syntaxError;
        public char must;   // + or -
        public String val;  // the field value (minus the field name, +/-, quotes)
        public String raw;  // the raw clause w/o leading/trailing whitespace
        public float boost = 1.0f;
    }

    public static List<Clause> splitIntoClauses(String s, boolean ignoreQuote) {
        ArrayList<Clause> lst = new ArrayList<>(4);
        Clause clause;

        int pos=0;
        int end=s.length();
        char ch=0;
        int start;
        boolean disallowUserField;
        while (pos < end) {
            clause = new Clause();
            disallowUserField = true;

            ch = s.charAt(pos);

            while (Character.isWhitespace(ch)) {
                if (++pos >= end) break;
                ch = s.charAt(pos);
            }

            start = pos;

            if ((ch=='+' || ch=='-') && (pos+1)<end) {
                clause.must = ch;
                pos++;
            }

            clause.field = getFieldName(s, pos, end);
            if (clause.field != null) {
                disallowUserField = false;
                int colon = s.indexOf(':',pos);
                clause.rawField = s.substring(pos, colon);
                pos += colon - pos; // skip the field name
                pos++;  // skip the ':'
            }

            if (pos>=end) break;


            char inString=0;

            ch = s.charAt(pos);
            if (!ignoreQuote && ch=='"') {
                clause.isPhrase = true;
                inString = '"';
                pos++;
            }

            boolean forceBreak = false;
            StringBuilder sb = new StringBuilder();
            while (pos < end) {
                ch = s.charAt(pos++);
                if (ch=='\\') {    // skip escaped chars, but leave escaped
                    sb.append(ch);
                    if (pos >= end) {
                        sb.append(ch); // double backslash if we are at the end of the string
                        break;
                    }
                    ch = s.charAt(pos++);
                    sb.append(ch);
                    continue;
                } else if (inString != 0 && ch == inString) {
                    inString=0;
                    break;
                } else if (Character.isWhitespace(ch)) {
                    clause.hasWhitespace=true;
                    if (inString == 0) {
                        // end of the token if we aren't in a string, backing
                        // up the position.
                        pos--;
                        break;
                    }
                }

                if (inString == 0) {
                    switch (ch) {
                        case '!':
                        case '(':
                        case ')':
                        case ':':
                        case '[':
                        case ']':
                        case '{':
                        case '}':
                        case '^':
                        case '~':
                        case '*':
                        case '?':
                        case '"':
                        case '+':
                        case '-':
                        case '\\':
                        case '|':
                        case '&':
                        case '/':
                            clause.hasSpecialSyntax = true;
                    }
                }

                sb.append(ch);
            }
            clause.val = sb.toString();

            // Strip the boost from val and assign to local var
            if (clause.val.lastIndexOf('^') > 0) {
                clause.boost = new Float(clause.val.substring(clause.val.lastIndexOf('^') + 1));
                clause.val = clause.val.substring(0, clause.val.lastIndexOf('^'));
            }

            if (clause.isPhrase) {
                if (inString != 0) {
                    // detected bad quote balancing... retry
                    // parsing with quotes like any other char
                    return splitIntoClauses(s, true);
                }

                // special syntax in a string isn't special
                clause.hasSpecialSyntax = false;
            } else {
                // an empty clause... must be just a + or - on it's own
                if (clause.val.length() == 0) {
                    clause.syntaxError = true;
                    if (clause.must != 0) {
                        clause.val="\\"+clause.must;
                        clause.must = 0;
                        clause.hasSpecialSyntax = true;
                    } else {
                        // uh.. this shouldn't happen.
                        clause=null;
                    }
                }
            }

            if (clause != null) {
                if(disallowUserField) {
                    clause.raw = s.substring(start, pos);
                    // escape colons, except for "match all" query
                    if(!"*:*".equals(clause.raw)) {
                        clause.raw = clause.raw.replaceAll("([^\\\\]):", "$1\\\\:");
                    }
                } else {
                    clause.raw = s.substring(start, pos);
                    // OSC: We don't care about this as it will be picked up later
                    /*
                    // Add default userField boost if no explicit boost exists
                    if(config.userFields.isAllowed(clause.field) && !clause.raw.contains("^")) {
                        Float boost = config.userFields.getBoost(clause.field);
                        if(boost != null)
                            clause.raw += "^" + boost;
                    }
                    */
                }


                lst.add(clause);
            }
        }

        return lst;
    }

    /**
     * returns a field name or legal field alias from the current
     * position of the string
     */
    private static String getFieldName(String s, int pos, int end) {
        if (pos >= end) return null;
        int p=pos;
        int colon = s.indexOf(':',pos);
        // make sure there is space after the colon, but not whitespace
        if (colon<=pos || colon+1>=end || Character.isWhitespace(s.charAt(colon+1))) return null;
        char ch = s.charAt(p++);
        while ((ch=='(' || ch=='+' || ch=='-') && (pos<end)) {
            ch = s.charAt(p++);
            pos++;
        }
        if (!Character.isJavaIdentifierPart(ch)) return null;
        while (p<colon) {
            ch = s.charAt(p++);
            if (!(Character.isJavaIdentifierPart(ch) || ch=='-' || ch=='.')) return null;
        }
        String fname = s.substring(pos, p);
        return fname;
    }


}
