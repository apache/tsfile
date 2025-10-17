// Generated from org/apache/tsfile/parser/PathParser.g4 by ANTLR 4.9.3
package org.apache.tsfile.parser;
import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link PathParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface PathParserVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link PathParser#path}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPath(PathParser.PathContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#prefixPath}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitPrefixPath(PathParser.PrefixPathContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#suffixPath}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSuffixPath(PathParser.SuffixPathContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#nodeName}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeName(PathParser.NodeNameContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#nodeNameWithoutWildcard}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeNameWithoutWildcard(PathParser.NodeNameWithoutWildcardContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#nodeNameSlice}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitNodeNameSlice(PathParser.NodeNameSliceContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#identifier}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitIdentifier(PathParser.IdentifierContext ctx);
	/**
	 * Visit a parse tree produced by {@link PathParser#wildcard}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitWildcard(PathParser.WildcardContext ctx);
}