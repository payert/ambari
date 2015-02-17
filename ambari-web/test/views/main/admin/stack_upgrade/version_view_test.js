/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


var App = require('app');
require('views/main/admin/stack_upgrade/upgrade_wizard_view');

describe('App.mainAdminStackVersionsView', function () {
  var view = App.MainAdminStackVersionsView.create({
    controller: {
      currentVersion: {
        repository_version: "2.2.1.0"
      },
      load: Em.K
    }
  });

  describe("#filterBy()", function () {
    var versions = [
      Em.Object.create({
        status: "INIT"
      }),
      Em.Object.create({
        status: "INSTALLING"
      }),
      Em.Object.create({
        status: "INSTALLED",
        repositoryVersion: "2.2.0.1"
      }),
      Em.Object.create({
        status: "INSTALLED",
        repositoryVersion: "2.2.2.1"
      }),
      Em.Object.create({
        status: "INSTALL_FAILED"
      }),
      Em.Object.create({
        status: "OUT_OF_SYNC"
      }),
      Em.Object.create({
        status: "UPGRADING"
      }),
      Em.Object.create({
        status: "UPGRADED"
      }),
      Em.Object.create({
        status: "CURRENT"
      })
    ];

    var testCases = [
      {
        filter:  Em.Object.create({
          value: ''
        }),
        filteredVersions: [
          Em.Object.create({
            status: "INIT"
          }),
          Em.Object.create({
            status: "INSTALLING"
          }),
          Em.Object.create({
            status: "INSTALLED",
            repositoryVersion: "2.2.0.1"
          }),
          Em.Object.create({
            status: "INSTALLED",
            repositoryVersion: "2.2.2.1"
          }),
          Em.Object.create({
            status: "INSTALL_FAILED"
          }),
          Em.Object.create({
            status: "OUT_OF_SYNC"
          }),
          Em.Object.create({
            status: "UPGRADING"
          }),
          Em.Object.create({
            status: "UPGRADED"
          }),
          Em.Object.create({
            status: "CURRENT"
          })
        ]
      },
      {
        filter:  Em.Object.create({
          value: 'NOT_INSTALLED'
        }),
        filteredVersions: [
          Em.Object.create({
            status: "INIT"
          }),
          Em.Object.create({
            status: "INSTALLING"
          }),
          Em.Object.create({
            status: "INSTALL_FAILED"
          }),
          Em.Object.create({
            status: "OUT_OF_SYNC"
          })
        ]
      },
      {
        filter:  Em.Object.create({
          value: 'INSTALLED'
        }),
        filteredVersions: [
          Em.Object.create({
            status: "INSTALLED",
            repositoryVersion: "2.2.0.1"
          })
        ]
      },
      {
        filter:  Em.Object.create({
          value: 'UPGRADE_READY'
        }),
        filteredVersions: [
          Em.Object.create({
            status: "INSTALLED",
            repositoryVersion: "2.2.2.1"
          })
        ]
      },
      {
        filter:  Em.Object.create({
          value: 'CURRENT'
        }),
        filteredVersions: [
          Em.Object.create({
            status: "CURRENT"
          })
        ]
      },
      {
        filter:  Em.Object.create({
          value: 'UPGRADING'
        }),
        filteredVersions: [
          Em.Object.create({
            status: "UPGRADING"
          })
        ]
      },
      {
        filter:  Em.Object.create({
          value: 'UPGRADED'
        }),
        filteredVersions: [
          Em.Object.create({
            status: "UPGRADED"
          })
        ]
      }
    ].forEach(function(t) {
        var msg = t.filter.get('value') ? t.filter.get('value') : "All";
        it("filter By " + msg, function () {
          view.set('controller.currentVersion', {repository_version: '2.2.1.1'});
          expect(view.filterBy(versions, t.filter)).to.eql(t.filteredVersions);
        });
      });
  });

  describe("#didInsertElement()", function() {
    before(function () {
      sinon.stub(view, 'observesCategories', Em.K);
    });
    after(function () {
      view.observesCategories.restore();
    });
    it("", function() {
      view.didInsertElement();
      expect(view.observesCategories.calledOnce).to.be.true;
    });
  });

  describe("#observesCategories()", function () {
    var mock = {format: Em.K};
    before(function () {
      sinon.stub(Em.I18n, 't').returns(mock);
      sinon.stub(mock, 'format').returns('label');
      sinon.stub(view, 'filterBy').returns([]);
      sinon.stub(view, 'filterVersions');
    });
    after(function () {
      Em.I18n.t.restore();
      mock.format.restore();
      view.filterBy.restore();
      view.filterVersions.restore();
    });
    it("", function () {
      view.set('categories', [
        Em.Object.create({
          labelKey: 'labelKey',
          value: 'value',
          isSelected: false
        })
      ]);
      view.observesCategories();
      expect(view.get('categories')[0].get('label')).to.equal('label');
      expect(view.filterVersions.calledOnce).to.be.true;
    });
  });

  describe("#selectCategory()", function() {
    before(function () {
      sinon.stub(view, 'filterVersions');
    });
    after(function () {
      view.filterVersions.restore();
    });
    it("", function() {
      var event = {
        context: Em.Object.create({
          isSelected: false
        })
      };
      view.set('categories', [
        Em.Object.create({
          isSelected: true
        }),
        event.context
      ]);
      view.selectCategory(event);
      expect(view.get('categories')[0].get('isSelected')).to.be.false;
      expect(event.context.get('isSelected')).to.be.true;
      expect(view.filterVersions.calledWith(event.context)).to.be.true;
    });
  });

  describe("#filterVersions()", function() {
    before(function () {
      sinon.stub(view, 'filterBy').returns([{id: 1}]);
      sinon.stub(view, 'observesCategories', Em.K);
    });
    after(function () {
      view.filterBy.restore();
      view.observesCategories.restore();
    });
    it("", function() {
      view.set('repoVersions', [Em.Object.create({id: 1})]);
      view.filterVersions();
      expect(view.get('repoVersions')[0].get('isVisible')).to.be.true;
    });
  });

  describe("#goToVersions()", function() {
    before(function () {
      sinon.spy(App, 'showConfirmationPopup', Em.K);
      sinon.stub(window.location, 'replace', Em.K);
    });
    after(function () {
      App.showConfirmationPopup.restore();
      window.location.replace.restore();
    });
    it("", function() {
      var popup = view.goToVersions();
      expect(App.showConfirmationPopup.calledOnce).to.be.true;
      popup.onPrimary();
      expect(window.location.replace.calledWith('/views/ADMIN_VIEW/2.0.0/INSTANCE/#/stackVersions')).to.be.true;
    });
  });

  describe("#willInsertElement()", function() {
    before(function () {
      sinon.stub(view, 'doPolling', Em.K);
    });
    after(function () {
      view.doPolling.restore();
    });
    it("", function() {
      view.willInsertElement();
      expect(view.doPolling.calledOnce).to.be.true;
    });
  });

  describe("#willDestroyElement()", function() {
    before(function () {
      sinon.stub(window, 'clearTimeout', Em.K);
    });
    after(function () {
      window.clearTimeout.restore();
    });
    it("", function() {
      view.willDestroyElement();
      expect(window.clearTimeout.calledOnce).to.be.true;
    });
  });

  describe("#doPolling()", function() {
    before(function () {
      sinon.stub(window, 'setTimeout', Em.K);
    });
    after(function () {
      window.setTimeout.restore();
    });
    it("", function() {
      view.doPolling();
      expect(window.setTimeout.calledOnce).to.be.true;
    });
  });

  describe("#poll()", function() {
    beforeEach(function () {
      this.mock = sinon.stub(App.router, 'get');
      sinon.stub(view.get('controller'), 'load').returns({done: Em.K});
    });
    afterEach(function () {
      this.mock.restore();
      view.get('controller').load.restore();
    });
    it("update is working", function() {
      this.mock.returns(Em.Object.create({isWorking: true}));
      view.poll();
      expect(view.get('controller').load.calledOnce).to.be.true;
    });
    it("update is not working", function() {
      this.mock.returns(Em.Object.create({isWorking: false}));
      view.poll();
      expect(view.get('controller').load.called).to.be.false;
    });
  });
});
