// Copyright (c) rubicon IT GmbH, www.rubicon.eu
//
// See the NOTICE file distributed with this work for additional information
// regarding copyright ownership.  rubicon licenses this file to you under 
// the Apache License, Version 2.0 (the "License"); you may not use this 
// file except in compliance with the License.  You may obtain a copy of the 
// License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software 
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT 
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the 
// License for the specific language governing permissions and limitations
// under the License.
// 
using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Remotion.Linq.Clauses;
using Remotion.Linq.Clauses.Expressions;
using Remotion.Linq.UnitTests.TestDomain;

namespace Remotion.Linq.UnitTests.Parsing.Structure.QueryParserIntegrationTests
{
  [TestFixture]
  public class GroupJoinQueryParserIntegrationTest : QueryParserIntegrationTestBase
  {
    [Test]
    public void GroupJoin ()
    {
      var query = from s in QuerySource
                  join sd in DetailQuerySource on s.ID equals sd.RoomNumber into sds
                  select Tuple.Create (s, sds);

      var queryModel = QueryParser.GetParsedQuery (query.Expression);
      //from Cook s in TestQueryable<Cook>() join Kitchen sd in TestQueryable<Kitchen>() on [s].ID equals [sd].RoomNumber into IEnumerable`1 sds select Create([s], [sds])
      Console.WriteLine (queryModel.ToString());
      Assert.That (queryModel.GetOutputDataInfo ().DataType, Is.SameAs (typeof (IQueryable<Tuple<Cook, IEnumerable<Kitchen>>>)));

      var mainFromClause = queryModel.MainFromClause;
      CheckConstantQuerySource (mainFromClause.FromExpression, QuerySource);
      Assert.That (mainFromClause.ItemType, Is.SameAs (typeof (Cook)));
      Assert.That (mainFromClause.ItemName, Is.EqualTo ("s"));

      var groupJoinClause = ((GroupJoinClause) queryModel.BodyClauses[0]);
      Assert.That (groupJoinClause.ItemName, Is.SameAs ("sds"));
      Assert.That (groupJoinClause.ItemType, Is.SameAs (typeof (IEnumerable<Kitchen>)));
      CheckConstantQuerySource (groupJoinClause.JoinClause.InnerSequence, DetailQuerySource);
      Assert.That (groupJoinClause.JoinClause.ItemType, Is.SameAs (typeof (Kitchen)));
      Assert.That (groupJoinClause.JoinClause.ItemName, Is.EqualTo ("sd"));
      CheckResolvedExpression<Cook, int> (groupJoinClause.JoinClause.OuterKeySelector, mainFromClause, s => s.ID);
      CheckResolvedExpression<Kitchen, int> (groupJoinClause.JoinClause.InnerKeySelector, groupJoinClause.JoinClause, sd => sd.RoomNumber);

      var selectClause = queryModel.SelectClause;
      CheckResolvedExpression<Cook, IEnumerable<Kitchen>, Tuple<Cook, IEnumerable<Kitchen>>> (
          selectClause.Selector,
          mainFromClause,
          groupJoinClause,
          (s, sds) => Tuple.Create (s, sds));
    }

    [Test]
    [Explicit]
    public void GroupJoin_WithSubQueryAsFirstQuerySource ()
    {
      var query = from s in QuerySource.Take (2)
                  join sd in DetailQuerySource on s.ID equals sd.RoomNumber into sds
                  select Tuple.Create (s, sds);

      var queryModel = QueryParser.GetParsedQuery (query.Expression);
      //Expected: from Cook <generated>_1 in {TestQueryable<Cook>() => Take(2)} join Kitchen sd in TestQueryable<Kitchen>() on [<generated>_1].ID equals [sd].RoomNumber into IEnumerable`1 sds select Create([<generated>_1], [sds])
      //Actual:   from Cook s in {TestQueryable<Cook>() => Take(2)} join Kitchen sd in TestQueryable<Kitchen>() on [<generated>_1].ID equals [sd].RoomNumber into IEnumerable`1 sds select Create([s], [sds])
      Console.WriteLine (queryModel.ToString());
      Assert.That (queryModel.GetOutputDataInfo ().DataType, Is.SameAs (typeof (IQueryable<Tuple<Cook, IEnumerable<Kitchen>>>)));

      var mainFromClause = queryModel.MainFromClause;
      Assert.That (mainFromClause.ItemType, Is.SameAs (typeof (Cook)));
      Assert.That (mainFromClause.ItemName, Is.EqualTo ("<generated>_1"));

      var subQueryModel = ((SubQueryExpression) mainFromClause.FromExpression).QueryModel;
      var subQueryMainFromClause = subQueryModel.MainFromClause;
      Assert.That (subQueryMainFromClause.ItemType, Is.SameAs (typeof (Cook)));
      Assert.That (subQueryMainFromClause.ItemName, Is.EqualTo ("<generated>_1"));

      var groupJoinClause = ((GroupJoinClause) queryModel.BodyClauses[0]);
      Assert.That (groupJoinClause.ItemName, Is.SameAs ("sds"));
      Assert.That (groupJoinClause.ItemType, Is.SameAs (typeof (IEnumerable<Kitchen>)));
      CheckConstantQuerySource (groupJoinClause.JoinClause.InnerSequence, DetailQuerySource);
      Assert.That (groupJoinClause.JoinClause.ItemType, Is.SameAs (typeof (Kitchen)));
      Assert.That (groupJoinClause.JoinClause.ItemName, Is.EqualTo ("sd"));
      CheckResolvedExpression<Cook, int> (groupJoinClause.JoinClause.OuterKeySelector, mainFromClause, s => s.ID);
      CheckResolvedExpression<Kitchen, int> (groupJoinClause.JoinClause.InnerKeySelector, groupJoinClause.JoinClause, sd => sd.RoomNumber);

      var selectClause = queryModel.SelectClause;
      CheckResolvedExpression<Cook, IEnumerable<Kitchen>, Tuple<Cook, IEnumerable<Kitchen>>> (
          selectClause.Selector,
          mainFromClause,
          groupJoinClause,
          (s, sds) => Tuple.Create (s, sds));
    }

    [Test]
    [Explicit]
    public void GroupJoin_1 ()
    {
      var query = from s in QuerySource
        join sd in DetailQuerySource on s.ID equals sd.RoomNumber into sds
        from k in sds.DefaultIfEmpty()
        select k;

      var queryModel = QueryParser.GetParsedQuery (query.Expression);
      //Expected: from Cook <generated>_1 in {TestQueryable<Cook>() => Take(1)} join Kitchen sd in TestQueryable<Kitchen>() on [s].ID equals [sd].RoomNumber into IEnumerable`1 sds from Kitchen k in {[sds] => DefaultIfEmpty()} select [k]
      //Actual:   from Cook s in {TestQueryable<Cook>() => Take(1)} join Kitchen sd in TestQueryable<Kitchen>() on [<generated>_1].ID equals [sd].RoomNumber into IEnumerable`1 sds from Kitchen k in {[sds] => DefaultIfEmpty()} select [k]
      Console.WriteLine (queryModel.ToString());
    }

    [Test]
    public void GroupJoin_WithoutSelect ()
    {
      var query = QuerySource.GroupJoin (DetailQuerySource, s => s.ID, sd => sd.RoomNumber, (s, sds) => Tuple.Create (s, sds));

      var queryModel = QueryParser.GetParsedQuery (query.Expression);
      Assert.That (queryModel.GetOutputDataInfo ().DataType, Is.SameAs (typeof (IQueryable<Tuple<Cook, IEnumerable<Kitchen>>>)));

      var mainFromClause = queryModel.MainFromClause;
      CheckConstantQuerySource (mainFromClause.FromExpression, QuerySource);
      Assert.That (mainFromClause.ItemType, Is.SameAs (typeof (Cook)));
      Assert.That (mainFromClause.ItemName, Is.EqualTo ("s"));

      var groupJoinClause = ((GroupJoinClause) queryModel.BodyClauses[0]);
      Assert.That (groupJoinClause.ItemName, Is.SameAs ("sds"));
      Assert.That (groupJoinClause.ItemType, Is.SameAs (typeof (IEnumerable<Kitchen>)));
      CheckConstantQuerySource (groupJoinClause.JoinClause.InnerSequence, DetailQuerySource);
      Assert.That (groupJoinClause.JoinClause.ItemType, Is.SameAs (typeof (Kitchen)));
      Assert.That (groupJoinClause.JoinClause.ItemName, Is.EqualTo ("sd"));
      CheckResolvedExpression<Cook, int> (groupJoinClause.JoinClause.OuterKeySelector, mainFromClause, s => s.ID);
      CheckResolvedExpression<Kitchen, int> (groupJoinClause.JoinClause.InnerKeySelector, groupJoinClause.JoinClause, sd => sd.RoomNumber);

      var selectClause = queryModel.SelectClause;
      CheckResolvedExpression<Cook, IEnumerable<Kitchen>, Tuple<Cook, IEnumerable<Kitchen>>> (
          selectClause.Selector,
          mainFromClause,
          groupJoinClause,
          (s, sds) => Tuple.Create (s, sds));
    }
  }
}
