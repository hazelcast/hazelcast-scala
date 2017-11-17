package com.hazelcast.Scala

import com.hazelcast.query.SqlPredicate

private[Scala] object Macros {
  import reflect.macros.whitebox.Context
  def Where(c: Context)(args: c.Expr[Any]*): c.Expr[SqlPredicate] = {
    import c.universe._
    c.prefix.tree match {
      case Apply(_, List(Apply(_, rawParts))) =>
        val parts = rawParts map { case t @ Literal(Constant(const: String)) => (const, t.pos) }
        parts match {
          case List((raw, pos)) =>
            try {
              new SqlPredicate(raw)
              c.Expr[SqlPredicate](q" new com.hazelcast.query.SqlPredicate($raw) ")
            } catch {
              case e: RuntimeException =>
                c.error(pos, e.getMessage)
                c.Expr[SqlPredicate](q"")
            }
          case Nil =>
            c.abort(c.enclosingPosition, "Unknown error")
          case _ =>
            c.Expr[SqlPredicate](q" new com.hazelcast.query.SqlPredicate( StringContext(..$rawParts).raw(..$args) ) ")
        }
      case _ =>
        c.abort(c.enclosingPosition, "Unknown error")
    }
  }

}
