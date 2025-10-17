// Generated from org/apache/tsfile/parser/PathParser.g4 by ANTLR 4.9.3
package org.apache.tsfile.parser;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class PathParser extends Parser {
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
	public static final int
		RULE_path = 0, RULE_prefixPath = 1, RULE_suffixPath = 2, RULE_nodeName = 3, 
		RULE_nodeNameWithoutWildcard = 4, RULE_nodeNameSlice = 5, RULE_identifier = 6, 
		RULE_wildcard = 7;
	private static String[] makeRuleNames() {
		return new String[] {
			"path", "prefixPath", "suffixPath", "nodeName", "nodeNameWithoutWildcard", 
			"nodeNameSlice", "identifier", "wildcard"
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

	@Override
	public String getGrammarFileName() { return "PathParser.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public PathParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	public static class PathContext extends ParserRuleContext {
		public PrefixPathContext prefixPath() {
			return getRuleContext(PrefixPathContext.class,0);
		}
		public TerminalNode EOF() { return getToken(PathParser.EOF, 0); }
		public SuffixPathContext suffixPath() {
			return getRuleContext(SuffixPathContext.class,0);
		}
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_path);
		try {
			setState(22);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ROOT:
				enterOuterAlt(_localctx, 1);
				{
				setState(16);
				prefixPath();
				setState(17);
				match(EOF);
				}
				break;
			case STAR:
			case DOUBLE_STAR:
			case DURATION_LITERAL:
			case INTEGER_LITERAL:
			case ID:
			case QUOTED_ID:
				enterOuterAlt(_localctx, 2);
				{
				setState(19);
				suffixPath();
				setState(20);
				match(EOF);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PrefixPathContext extends ParserRuleContext {
		public TerminalNode ROOT() { return getToken(PathParser.ROOT, 0); }
		public List<TerminalNode> DOT() { return getTokens(PathParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(PathParser.DOT, i);
		}
		public List<NodeNameContext> nodeName() {
			return getRuleContexts(NodeNameContext.class);
		}
		public NodeNameContext nodeName(int i) {
			return getRuleContext(NodeNameContext.class,i);
		}
		public PrefixPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prefixPath; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitPrefixPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final PrefixPathContext prefixPath() throws RecognitionException {
		PrefixPathContext _localctx = new PrefixPathContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_prefixPath);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(24);
			match(ROOT);
			setState(29);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(25);
				match(DOT);
				setState(26);
				nodeName();
				}
				}
				setState(31);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class SuffixPathContext extends ParserRuleContext {
		public List<NodeNameContext> nodeName() {
			return getRuleContexts(NodeNameContext.class);
		}
		public NodeNameContext nodeName(int i) {
			return getRuleContext(NodeNameContext.class,i);
		}
		public List<TerminalNode> DOT() { return getTokens(PathParser.DOT); }
		public TerminalNode DOT(int i) {
			return getToken(PathParser.DOT, i);
		}
		public SuffixPathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_suffixPath; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitSuffixPath(this);
			else return visitor.visitChildren(this);
		}
	}

	public final SuffixPathContext suffixPath() throws RecognitionException {
		SuffixPathContext _localctx = new SuffixPathContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_suffixPath);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(32);
			nodeName();
			setState(37);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==DOT) {
				{
				{
				setState(33);
				match(DOT);
				setState(34);
				nodeName();
				}
				}
				setState(39);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeNameContext extends ParserRuleContext {
		public List<WildcardContext> wildcard() {
			return getRuleContexts(WildcardContext.class);
		}
		public WildcardContext wildcard(int i) {
			return getRuleContext(WildcardContext.class,i);
		}
		public NodeNameSliceContext nodeNameSlice() {
			return getRuleContext(NodeNameSliceContext.class,0);
		}
		public NodeNameWithoutWildcardContext nodeNameWithoutWildcard() {
			return getRuleContext(NodeNameWithoutWildcardContext.class,0);
		}
		public NodeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeName; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitNodeName(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeNameContext nodeName() throws RecognitionException {
		NodeNameContext _localctx = new NodeNameContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_nodeName);
		int _la;
		try {
			setState(50);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(40);
				wildcard();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(41);
				wildcard();
				setState(42);
				nodeNameSlice();
				setState(44);
				_errHandler.sync(this);
				_la = _input.LA(1);
				if (_la==STAR || _la==DOUBLE_STAR) {
					{
					setState(43);
					wildcard();
					}
				}

				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(46);
				nodeNameSlice();
				setState(47);
				wildcard();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(49);
				nodeNameWithoutWildcard();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeNameWithoutWildcardContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public NodeNameWithoutWildcardContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeNameWithoutWildcard; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitNodeNameWithoutWildcard(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeNameWithoutWildcardContext nodeNameWithoutWildcard() throws RecognitionException {
		NodeNameWithoutWildcardContext _localctx = new NodeNameWithoutWildcardContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_nodeNameWithoutWildcard);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(52);
			identifier();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeNameSliceContext extends ParserRuleContext {
		public IdentifierContext identifier() {
			return getRuleContext(IdentifierContext.class,0);
		}
		public TerminalNode INTEGER_LITERAL() { return getToken(PathParser.INTEGER_LITERAL, 0); }
		public NodeNameSliceContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nodeNameSlice; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitNodeNameSlice(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NodeNameSliceContext nodeNameSlice() throws RecognitionException {
		NodeNameSliceContext _localctx = new NodeNameSliceContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_nodeNameSlice);
		try {
			setState(56);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case DURATION_LITERAL:
			case ID:
			case QUOTED_ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(54);
				identifier();
				}
				break;
			case INTEGER_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(55);
				match(INTEGER_LITERAL);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IdentifierContext extends ParserRuleContext {
		public TerminalNode DURATION_LITERAL() { return getToken(PathParser.DURATION_LITERAL, 0); }
		public TerminalNode ID() { return getToken(PathParser.ID, 0); }
		public TerminalNode QUOTED_ID() { return getToken(PathParser.QUOTED_ID, 0); }
		public IdentifierContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_identifier; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitIdentifier(this);
			else return visitor.visitChildren(this);
		}
	}

	public final IdentifierContext identifier() throws RecognitionException {
		IdentifierContext _localctx = new IdentifierContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_identifier);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(58);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << DURATION_LITERAL) | (1L << ID) | (1L << QUOTED_ID))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class WildcardContext extends ParserRuleContext {
		public TerminalNode STAR() { return getToken(PathParser.STAR, 0); }
		public TerminalNode DOUBLE_STAR() { return getToken(PathParser.DOUBLE_STAR, 0); }
		public WildcardContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_wildcard; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof PathParserVisitor ) return ((PathParserVisitor<? extends T>)visitor).visitWildcard(this);
			else return visitor.visitChildren(this);
		}
	}

	public final WildcardContext wildcard() throws RecognitionException {
		WildcardContext _localctx = new WildcardContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_wildcard);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(60);
			_la = _input.LA(1);
			if ( !(_la==STAR || _la==DOUBLE_STAR) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3\'A\4\2\t\2\4\3\t"+
		"\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\3\2\3\2\3\2\3\2\3\2"+
		"\3\2\5\2\31\n\2\3\3\3\3\3\3\7\3\36\n\3\f\3\16\3!\13\3\3\4\3\4\3\4\7\4"+
		"&\n\4\f\4\16\4)\13\4\3\5\3\5\3\5\3\5\5\5/\n\5\3\5\3\5\3\5\3\5\5\5\65\n"+
		"\5\3\6\3\6\3\7\3\7\5\7;\n\7\3\b\3\b\3\t\3\t\3\t\2\2\n\2\4\6\b\n\f\16\20"+
		"\2\4\4\2\"\"&\'\3\2\32\33\2@\2\30\3\2\2\2\4\32\3\2\2\2\6\"\3\2\2\2\b\64"+
		"\3\2\2\2\n\66\3\2\2\2\f:\3\2\2\2\16<\3\2\2\2\20>\3\2\2\2\22\23\5\4\3\2"+
		"\23\24\7\2\2\3\24\31\3\2\2\2\25\26\5\6\4\2\26\27\7\2\2\3\27\31\3\2\2\2"+
		"\30\22\3\2\2\2\30\25\3\2\2\2\31\3\3\2\2\2\32\37\7\3\2\2\33\34\7\27\2\2"+
		"\34\36\5\b\5\2\35\33\3\2\2\2\36!\3\2\2\2\37\35\3\2\2\2\37 \3\2\2\2 \5"+
		"\3\2\2\2!\37\3\2\2\2\"\'\5\b\5\2#$\7\27\2\2$&\5\b\5\2%#\3\2\2\2&)\3\2"+
		"\2\2\'%\3\2\2\2\'(\3\2\2\2(\7\3\2\2\2)\'\3\2\2\2*\65\5\20\t\2+,\5\20\t"+
		"\2,.\5\f\7\2-/\5\20\t\2.-\3\2\2\2./\3\2\2\2/\65\3\2\2\2\60\61\5\f\7\2"+
		"\61\62\5\20\t\2\62\65\3\2\2\2\63\65\5\n\6\2\64*\3\2\2\2\64+\3\2\2\2\64"+
		"\60\3\2\2\2\64\63\3\2\2\2\65\t\3\2\2\2\66\67\5\16\b\2\67\13\3\2\2\28;"+
		"\5\16\b\29;\7$\2\2:8\3\2\2\2:9\3\2\2\2;\r\3\2\2\2<=\t\2\2\2=\17\3\2\2"+
		"\2>?\t\3\2\2?\21\3\2\2\2\b\30\37\'.\64:";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}