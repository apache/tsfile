// Generated from org/apache/tsfile/parser/PathLexer.g4 by ANTLR 4.9.3
package org.apache.tsfile.parser;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PathLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.9.3", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		ROOT=1, WS=2, TIME=3, TIMESTAMP=4, MINUS=5, PLUS=6, DIV=7, MOD=8, OPERATOR_DEQ=9, 
		OPERATOR_SEQ=10, OPERATOR_GT=11, OPERATOR_GTE=12, OPERATOR_LT=13, OPERATOR_LTE=14, 
		OPERATOR_NEQ=15, OPERATOR_BITWISE_AND=16, OPERATOR_LOGICAL_AND=17, OPERATOR_BITWISE_OR=18, 
		OPERATOR_LOGICAL_OR=19, OPERATOR_NOT=20, DOT=21, COMMA=22, SEMI=23, STAR=24, 
		DOUBLE_STAR=25, LR_BRACKET=26, RR_BRACKET=27, LS_BRACKET=28, RS_BRACKET=29, 
		DOUBLE_COLON=30, STRING_LITERAL=31, DURATION_LITERAL=32, DATETIME_LITERAL=33, 
		INTEGER_LITERAL=34, EXPONENT_NUM_PART=35, ID=36, QUOTED_ID=37;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	private static String[] makeRuleNames() {
		return new String[] {
			"ROOT", "WS", "TIME", "TIMESTAMP", "MINUS", "PLUS", "DIV", "MOD", "OPERATOR_DEQ", 
			"OPERATOR_SEQ", "OPERATOR_GT", "OPERATOR_GTE", "OPERATOR_LT", "OPERATOR_LTE", 
			"OPERATOR_NEQ", "OPERATOR_BITWISE_AND", "OPERATOR_LOGICAL_AND", "OPERATOR_BITWISE_OR", 
			"OPERATOR_LOGICAL_OR", "OPERATOR_NOT", "DOT", "COMMA", "SEMI", "STAR", 
			"DOUBLE_STAR", "LR_BRACKET", "RR_BRACKET", "LS_BRACKET", "RS_BRACKET", 
			"DOUBLE_COLON", "STRING_LITERAL", "DURATION_LITERAL", "DATETIME_LITERAL", 
			"DATE_LITERAL", "TIME_LITERAL", "INTEGER_LITERAL", "EXPONENT_NUM_PART", 
			"DEC_DIGIT", "ID", "QUOTED_ID", "NAME_CHAR", "CN_CHAR", "DQUOTA_STRING", 
			"SQUOTA_STRING", "BQUOTA_STRING", "A", "B", "C", "D", "E", "F", "G", 
			"H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", 
			"V", "W", "X", "Y", "Z"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, null, null, null, null, "'-'", "'+'", "'/'", "'%'", "'=='", "'='", 
			"'>'", "'>='", "'<'", "'<='", null, "'&'", "'&&'", "'|'", "'||'", "'!'", 
			"'.'", "','", "';'", "'*'", "'**'", "'('", "')'", "'['", "']'", "'::'"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "ROOT", "WS", "TIME", "TIMESTAMP", "MINUS", "PLUS", "DIV", "MOD", 
			"OPERATOR_DEQ", "OPERATOR_SEQ", "OPERATOR_GT", "OPERATOR_GTE", "OPERATOR_LT", 
			"OPERATOR_LTE", "OPERATOR_NEQ", "OPERATOR_BITWISE_AND", "OPERATOR_LOGICAL_AND", 
			"OPERATOR_BITWISE_OR", "OPERATOR_LOGICAL_OR", "OPERATOR_NOT", "DOT", 
			"COMMA", "SEMI", "STAR", "DOUBLE_STAR", "LR_BRACKET", "RR_BRACKET", "LS_BRACKET", 
			"RS_BRACKET", "DOUBLE_COLON", "STRING_LITERAL", "DURATION_LITERAL", "DATETIME_LITERAL", 
			"INTEGER_LITERAL", "EXPONENT_NUM_PART", "ID", "QUOTED_ID"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public PathLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "PathLexer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\'\u01b0\b\1\4\2\t"+
		"\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31\t\31"+
		"\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t \4!"+
		"\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t+\4"+
		",\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64\t"+
		"\64\4\65\t\65\4\66\t\66\4\67\t\67\48\t8\49\t9\4:\t:\4;\t;\4<\t<\4=\t="+
		"\4>\t>\4?\t?\4@\t@\4A\tA\4B\tB\4C\tC\4D\tD\4E\tE\4F\tF\4G\tG\4H\tH\3\2"+
		"\3\2\3\2\3\2\3\2\3\3\6\3\u0098\n\3\r\3\16\3\u0099\3\3\3\3\3\4\3\4\3\4"+
		"\3\4\3\4\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3"+
		"\b\3\t\3\t\3\n\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\17\3"+
		"\17\3\17\3\20\3\20\3\20\3\20\5\20\u00c8\n\20\3\21\3\21\3\22\3\22\3\22"+
		"\3\23\3\23\3\24\3\24\3\24\3\25\3\25\3\26\3\26\3\27\3\27\3\30\3\30\3\31"+
		"\3\31\3\32\3\32\3\32\3\33\3\33\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37"+
		"\3\37\3 \3 \5 \u00ee\n \3!\6!\u00f1\n!\r!\16!\u00f2\3!\3!\3!\3!\3!\3!"+
		"\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\3!\5!\u0107\n!\6!\u0109\n!\r!\16!\u010a"+
		"\3\"\3\"\3\"\5\"\u0110\n\"\3\"\3\"\3\"\3\"\3\"\3\"\5\"\u0118\n\"\5\"\u011a"+
		"\n\"\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\3#\5#\u012e\n"+
		"#\3$\3$\3$\3$\3$\3$\3$\3$\5$\u0138\n$\3%\6%\u013b\n%\r%\16%\u013c\3&\6"+
		"&\u0140\n&\r&\16&\u0141\3&\3&\5&\u0146\n&\3&\6&\u0149\n&\r&\16&\u014a"+
		"\3\'\3\'\3(\6(\u0150\n(\r(\16(\u0151\3)\3)\3*\3*\5*\u0158\n*\3+\3+\3,"+
		"\3,\3,\3,\7,\u0160\n,\f,\16,\u0163\13,\3,\3,\3-\3-\3-\3-\7-\u016b\n-\f"+
		"-\16-\u016e\13-\3-\3-\3.\3.\3.\3.\7.\u0176\n.\f.\16.\u0179\13.\3.\3.\3"+
		"/\3/\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\64\3\64\3\65\3\65\3\66"+
		"\3\66\3\67\3\67\38\38\39\39\3:\3:\3;\3;\3<\3<\3=\3=\3>\3>\3?\3?\3@\3@"+
		"\3A\3A\3B\3B\3C\3C\3D\3D\3E\3E\3F\3F\3G\3G\3H\3H\2\2I\3\3\5\4\7\5\t\6"+
		"\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23%\24"+
		"\'\25)\26+\27-\30/\31\61\32\63\33\65\34\67\359\36;\37= ?!A\"C#E\2G\2I"+
		"$K%M\2O&Q\'S\2U\2W\2Y\2[\2]\2_\2a\2c\2e\2g\2i\2k\2m\2o\2q\2s\2u\2w\2y"+
		"\2{\2}\2\177\2\u0081\2\u0083\2\u0085\2\u0087\2\u0089\2\u008b\2\u008d\2"+
		"\u008f\2\3\2#\5\2\13\r\17\17\"\"\4\2--//\4\2GGgg\3\2\62;\b\2%&\62<B\\"+
		"aac}\177\177\3\2$$\3\2))\3\2bb\4\2CCcc\4\2DDdd\4\2EEee\4\2FFff\4\2HHh"+
		"h\4\2IIii\4\2JJjj\4\2KKkk\4\2LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2"+
		"QQqq\4\2RRrr\4\2SSss\4\2TTtt\4\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4"+
		"\2ZZzz\4\2[[{{\4\2\\\\||\2\u01ad\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2"+
		"\t\3\2\2\2\2\13\3\2\2\2\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2"+
		"\2\2\2\25\3\2\2\2\2\27\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2"+
		"\2\37\3\2\2\2\2!\3\2\2\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2"+
		"\2+\3\2\2\2\2-\3\2\2\2\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\2\65\3\2\2"+
		"\2\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2A\3\2\2\2"+
		"\2C\3\2\2\2\2I\3\2\2\2\2K\3\2\2\2\2O\3\2\2\2\2Q\3\2\2\2\3\u0091\3\2\2"+
		"\2\5\u0097\3\2\2\2\7\u009d\3\2\2\2\t\u00a2\3\2\2\2\13\u00ac\3\2\2\2\r"+
		"\u00ae\3\2\2\2\17\u00b0\3\2\2\2\21\u00b2\3\2\2\2\23\u00b4\3\2\2\2\25\u00b7"+
		"\3\2\2\2\27\u00b9\3\2\2\2\31\u00bb\3\2\2\2\33\u00be\3\2\2\2\35\u00c0\3"+
		"\2\2\2\37\u00c7\3\2\2\2!\u00c9\3\2\2\2#\u00cb\3\2\2\2%\u00ce\3\2\2\2\'"+
		"\u00d0\3\2\2\2)\u00d3\3\2\2\2+\u00d5\3\2\2\2-\u00d7\3\2\2\2/\u00d9\3\2"+
		"\2\2\61\u00db\3\2\2\2\63\u00dd\3\2\2\2\65\u00e0\3\2\2\2\67\u00e2\3\2\2"+
		"\29\u00e4\3\2\2\2;\u00e6\3\2\2\2=\u00e8\3\2\2\2?\u00ed\3\2\2\2A\u0108"+
		"\3\2\2\2C\u010c\3\2\2\2E\u012d\3\2\2\2G\u012f\3\2\2\2I\u013a\3\2\2\2K"+
		"\u013f\3\2\2\2M\u014c\3\2\2\2O\u014f\3\2\2\2Q\u0153\3\2\2\2S\u0157\3\2"+
		"\2\2U\u0159\3\2\2\2W\u015b\3\2\2\2Y\u0166\3\2\2\2[\u0171\3\2\2\2]\u017c"+
		"\3\2\2\2_\u017e\3\2\2\2a\u0180\3\2\2\2c\u0182\3\2\2\2e\u0184\3\2\2\2g"+
		"\u0186\3\2\2\2i\u0188\3\2\2\2k\u018a\3\2\2\2m\u018c\3\2\2\2o\u018e\3\2"+
		"\2\2q\u0190\3\2\2\2s\u0192\3\2\2\2u\u0194\3\2\2\2w\u0196\3\2\2\2y\u0198"+
		"\3\2\2\2{\u019a\3\2\2\2}\u019c\3\2\2\2\177\u019e\3\2\2\2\u0081\u01a0\3"+
		"\2\2\2\u0083\u01a2\3\2\2\2\u0085\u01a4\3\2\2\2\u0087\u01a6\3\2\2\2\u0089"+
		"\u01a8\3\2\2\2\u008b\u01aa\3\2\2\2\u008d\u01ac\3\2\2\2\u008f\u01ae\3\2"+
		"\2\2\u0091\u0092\5\177@\2\u0092\u0093\5y=\2\u0093\u0094\5y=\2\u0094\u0095"+
		"\5\u0083B\2\u0095\4\3\2\2\2\u0096\u0098\t\2\2\2\u0097\u0096\3\2\2\2\u0098"+
		"\u0099\3\2\2\2\u0099\u0097\3\2\2\2\u0099\u009a\3\2\2\2\u009a\u009b\3\2"+
		"\2\2\u009b\u009c\b\3\2\2\u009c\6\3\2\2\2\u009d\u009e\5\u0083B\2\u009e"+
		"\u009f\5m\67\2\u009f\u00a0\5u;\2\u00a0\u00a1\5e\63\2\u00a1\b\3\2\2\2\u00a2"+
		"\u00a3\5\u0083B\2\u00a3\u00a4\5m\67\2\u00a4\u00a5\5u;\2\u00a5\u00a6\5"+
		"e\63\2\u00a6\u00a7\5\u0081A\2\u00a7\u00a8\5\u0083B\2\u00a8\u00a9\5]/\2"+
		"\u00a9\u00aa\5u;\2\u00aa\u00ab\5{>\2\u00ab\n\3\2\2\2\u00ac\u00ad\7/\2"+
		"\2\u00ad\f\3\2\2\2\u00ae\u00af\7-\2\2\u00af\16\3\2\2\2\u00b0\u00b1\7\61"+
		"\2\2\u00b1\20\3\2\2\2\u00b2\u00b3\7\'\2\2\u00b3\22\3\2\2\2\u00b4\u00b5"+
		"\7?\2\2\u00b5\u00b6\7?\2\2\u00b6\24\3\2\2\2\u00b7\u00b8\7?\2\2\u00b8\26"+
		"\3\2\2\2\u00b9\u00ba\7@\2\2\u00ba\30\3\2\2\2\u00bb\u00bc\7@\2\2\u00bc"+
		"\u00bd\7?\2\2\u00bd\32\3\2\2\2\u00be\u00bf\7>\2\2\u00bf\34\3\2\2\2\u00c0"+
		"\u00c1\7>\2\2\u00c1\u00c2\7?\2\2\u00c2\36\3\2\2\2\u00c3\u00c4\7#\2\2\u00c4"+
		"\u00c8\7?\2\2\u00c5\u00c6\7>\2\2\u00c6\u00c8\7@\2\2\u00c7\u00c3\3\2\2"+
		"\2\u00c7\u00c5\3\2\2\2\u00c8 \3\2\2\2\u00c9\u00ca\7(\2\2\u00ca\"\3\2\2"+
		"\2\u00cb\u00cc\7(\2\2\u00cc\u00cd\7(\2\2\u00cd$\3\2\2\2\u00ce\u00cf\7"+
		"~\2\2\u00cf&\3\2\2\2\u00d0\u00d1\7~\2\2\u00d1\u00d2\7~\2\2\u00d2(\3\2"+
		"\2\2\u00d3\u00d4\7#\2\2\u00d4*\3\2\2\2\u00d5\u00d6\7\60\2\2\u00d6,\3\2"+
		"\2\2\u00d7\u00d8\7.\2\2\u00d8.\3\2\2\2\u00d9\u00da\7=\2\2\u00da\60\3\2"+
		"\2\2\u00db\u00dc\7,\2\2\u00dc\62\3\2\2\2\u00dd\u00de\7,\2\2\u00de\u00df"+
		"\7,\2\2\u00df\64\3\2\2\2\u00e0\u00e1\7*\2\2\u00e1\66\3\2\2\2\u00e2\u00e3"+
		"\7+\2\2\u00e38\3\2\2\2\u00e4\u00e5\7]\2\2\u00e5:\3\2\2\2\u00e6\u00e7\7"+
		"_\2\2\u00e7<\3\2\2\2\u00e8\u00e9\7<\2\2\u00e9\u00ea\7<\2\2\u00ea>\3\2"+
		"\2\2\u00eb\u00ee\5W,\2\u00ec\u00ee\5Y-\2\u00ed\u00eb\3\2\2\2\u00ed\u00ec"+
		"\3\2\2\2\u00ee@\3\2\2\2\u00ef\u00f1\5I%\2\u00f0\u00ef\3\2\2\2\u00f1\u00f2"+
		"\3\2\2\2\u00f2\u00f0\3\2\2\2\u00f2\u00f3\3\2\2\2\u00f3\u0106\3\2\2\2\u00f4"+
		"\u0107\5\u008dG\2\u00f5\u00f6\5u;\2\u00f6\u00f7\5y=\2\u00f7\u0107\3\2"+
		"\2\2\u00f8\u0107\5\u0089E\2\u00f9\u0107\5c\62\2\u00fa\u0107\5k\66\2\u00fb"+
		"\u0107\5u;\2\u00fc\u0107\5\u0081A\2\u00fd\u00fe\5u;\2\u00fe\u00ff\5\u0081"+
		"A\2\u00ff\u0107\3\2\2\2\u0100\u0101\5\u0085C\2\u0101\u0102\5\u0081A\2"+
		"\u0102\u0107\3\2\2\2\u0103\u0104\5w<\2\u0104\u0105\5\u0081A\2\u0105\u0107"+
		"\3\2\2\2\u0106\u00f4\3\2\2\2\u0106\u00f5\3\2\2\2\u0106\u00f8\3\2\2\2\u0106"+
		"\u00f9\3\2\2\2\u0106\u00fa\3\2\2\2\u0106\u00fb\3\2\2\2\u0106\u00fc\3\2"+
		"\2\2\u0106\u00fd\3\2\2\2\u0106\u0100\3\2\2\2\u0106\u0103\3\2\2\2\u0107"+
		"\u0109\3\2\2\2\u0108\u00f0\3\2\2\2\u0109\u010a\3\2\2\2\u010a\u0108\3\2"+
		"\2\2\u010a\u010b\3\2\2\2\u010bB\3\2\2\2\u010c\u0119\5E#\2\u010d\u0110"+
		"\5\u0083B\2\u010e\u0110\5\5\3\2\u010f\u010d\3\2\2\2\u010f\u010e\3\2\2"+
		"\2\u0110\u0111\3\2\2\2\u0111\u0117\5G$\2\u0112\u0113\t\3\2\2\u0113\u0114"+
		"\5I%\2\u0114\u0115\7<\2\2\u0115\u0116\5I%\2\u0116\u0118\3\2\2\2\u0117"+
		"\u0112\3\2\2\2\u0117\u0118\3\2\2\2\u0118\u011a\3\2\2\2\u0119\u010f\3\2"+
		"\2\2\u0119\u011a\3\2\2\2\u011aD\3\2\2\2\u011b\u011c\5I%\2\u011c\u011d"+
		"\7/\2\2\u011d\u011e\5I%\2\u011e\u011f\7/\2\2\u011f\u0120\5I%\2\u0120\u012e"+
		"\3\2\2\2\u0121\u0122\5I%\2\u0122\u0123\7\61\2\2\u0123\u0124\5I%\2\u0124"+
		"\u0125\7\61\2\2\u0125\u0126\5I%\2\u0126\u012e\3\2\2\2\u0127\u0128\5I%"+
		"\2\u0128\u0129\7\60\2\2\u0129\u012a\5I%\2\u012a\u012b\7\60\2\2\u012b\u012c"+
		"\5I%\2\u012c\u012e\3\2\2\2\u012d\u011b\3\2\2\2\u012d\u0121\3\2\2\2\u012d"+
		"\u0127\3\2\2\2\u012eF\3\2\2\2\u012f\u0130\5I%\2\u0130\u0131\7<\2\2\u0131"+
		"\u0132\5I%\2\u0132\u0133\7<\2\2\u0133\u0137\5I%\2\u0134\u0135\5+\26\2"+
		"\u0135\u0136\5I%\2\u0136\u0138\3\2\2\2\u0137\u0134\3\2\2\2\u0137\u0138"+
		"\3\2\2\2\u0138H\3\2\2\2\u0139\u013b\5M\'\2\u013a\u0139\3\2\2\2\u013b\u013c"+
		"\3\2\2\2\u013c\u013a\3\2\2\2\u013c\u013d\3\2\2\2\u013dJ\3\2\2\2\u013e"+
		"\u0140\5M\'\2\u013f\u013e\3\2\2\2\u0140\u0141\3\2\2\2\u0141\u013f\3\2"+
		"\2\2\u0141\u0142\3\2\2\2\u0142\u0143\3\2\2\2\u0143\u0145\t\4\2\2\u0144"+
		"\u0146\t\3\2\2\u0145\u0144\3\2\2\2\u0145\u0146\3\2\2\2\u0146\u0148\3\2"+
		"\2\2\u0147\u0149\5M\'\2\u0148\u0147\3\2\2\2\u0149\u014a\3\2\2\2\u014a"+
		"\u0148\3\2\2\2\u014a\u014b\3\2\2\2\u014bL\3\2\2\2\u014c\u014d\t\5\2\2"+
		"\u014dN\3\2\2\2\u014e\u0150\5S*\2\u014f\u014e\3\2\2\2\u0150\u0151\3\2"+
		"\2\2\u0151\u014f\3\2\2\2\u0151\u0152\3\2\2\2\u0152P\3\2\2\2\u0153\u0154"+
		"\5[.\2\u0154R\3\2\2\2\u0155\u0158\t\6\2\2\u0156\u0158\5U+\2\u0157\u0155"+
		"\3\2\2\2\u0157\u0156\3\2\2\2\u0158T\3\2\2\2\u0159\u015a\4\u2e82\ua001"+
		"\2\u015aV\3\2\2\2\u015b\u0161\7$\2\2\u015c\u015d\7$\2\2\u015d\u0160\7"+
		"$\2\2\u015e\u0160\n\7\2\2\u015f\u015c\3\2\2\2\u015f\u015e\3\2\2\2\u0160"+
		"\u0163\3\2\2\2\u0161\u015f\3\2\2\2\u0161\u0162\3\2\2\2\u0162\u0164\3\2"+
		"\2\2\u0163\u0161\3\2\2\2\u0164\u0165\7$\2\2\u0165X\3\2\2\2\u0166\u016c"+
		"\7)\2\2\u0167\u0168\7)\2\2\u0168\u016b\7)\2\2\u0169\u016b\n\b\2\2\u016a"+
		"\u0167\3\2\2\2\u016a\u0169\3\2\2\2\u016b\u016e\3\2\2\2\u016c\u016a\3\2"+
		"\2\2\u016c\u016d\3\2\2\2\u016d\u016f\3\2\2\2\u016e\u016c\3\2\2\2\u016f"+
		"\u0170\7)\2\2\u0170Z\3\2\2\2\u0171\u0177\7b\2\2\u0172\u0173\7b\2\2\u0173"+
		"\u0176\7b\2\2\u0174\u0176\n\t\2\2\u0175\u0172\3\2\2\2\u0175\u0174\3\2"+
		"\2\2\u0176\u0179\3\2\2\2\u0177\u0175\3\2\2\2\u0177\u0178\3\2\2\2\u0178"+
		"\u017a\3\2\2\2\u0179\u0177\3\2\2\2\u017a\u017b\7b\2\2\u017b\\\3\2\2\2"+
		"\u017c\u017d\t\n\2\2\u017d^\3\2\2\2\u017e\u017f\t\13\2\2\u017f`\3\2\2"+
		"\2\u0180\u0181\t\f\2\2\u0181b\3\2\2\2\u0182\u0183\t\r\2\2\u0183d\3\2\2"+
		"\2\u0184\u0185\t\4\2\2\u0185f\3\2\2\2\u0186\u0187\t\16\2\2\u0187h\3\2"+
		"\2\2\u0188\u0189\t\17\2\2\u0189j\3\2\2\2\u018a\u018b\t\20\2\2\u018bl\3"+
		"\2\2\2\u018c\u018d\t\21\2\2\u018dn\3\2\2\2\u018e\u018f\t\22\2\2\u018f"+
		"p\3\2\2\2\u0190\u0191\t\23\2\2\u0191r\3\2\2\2\u0192\u0193\t\24\2\2\u0193"+
		"t\3\2\2\2\u0194\u0195\t\25\2\2\u0195v\3\2\2\2\u0196\u0197\t\26\2\2\u0197"+
		"x\3\2\2\2\u0198\u0199\t\27\2\2\u0199z\3\2\2\2\u019a\u019b\t\30\2\2\u019b"+
		"|\3\2\2\2\u019c\u019d\t\31\2\2\u019d~\3\2\2\2\u019e\u019f\t\32\2\2\u019f"+
		"\u0080\3\2\2\2\u01a0\u01a1\t\33\2\2\u01a1\u0082\3\2\2\2\u01a2\u01a3\t"+
		"\34\2\2\u01a3\u0084\3\2\2\2\u01a4\u01a5\t\35\2\2\u01a5\u0086\3\2\2\2\u01a6"+
		"\u01a7\t\36\2\2\u01a7\u0088\3\2\2\2\u01a8\u01a9\t\37\2\2\u01a9\u008a\3"+
		"\2\2\2\u01aa\u01ab\t \2\2\u01ab\u008c\3\2\2\2\u01ac\u01ad\t!\2\2\u01ad"+
		"\u008e\3\2\2\2\u01ae\u01af\t\"\2\2\u01af\u0090\3\2\2\2\32\2\u0099\u00c7"+
		"\u00ed\u00f2\u0106\u010a\u010f\u0117\u0119\u012d\u0137\u013c\u0141\u0145"+
		"\u014a\u0151\u0157\u015f\u0161\u016a\u016c\u0175\u0177\3\2\3\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}